package hmda.data.browser.repositories

import akka.NotUsed
import akka.stream.scaladsl.Source
import hmda.data.browser.models.{QueryField, ModifiedLarEntity}
import monix.eval.Task
import slick.basic.DatabaseConfig
import slick.jdbc.{JdbcProfile, ResultSetConcurrency, ResultSetType}

class PostgresModifiedLarRepository(tableName: String,
                                    config: DatabaseConfig[JdbcProfile])
    extends ModifiedLarRepository {

  import config._
  import config.profile.api._

  private val columns: String =
    """id,
    	lei,
    	loan_type,
    	loan_purpose,
    	preapproval,
    	construction_method,
    	occupancy_type,
    	loan_amount,
    	action_taken_type,
    	state,
    	county,
    	tract,
      COALESCE(ethnicity_applicant_1, ''),
      COALESCE(ethnicity_applicant_2, ''),
      COALESCE(ethnicity_applicant_3, ''),
      COALESCE(ethnicity_applicant_4, ''),
      COALESCE(ethnicity_applicant_5, ''),
      COALESCE(CAST(ethnicity_observed_applicant as varchar), ''),
      COALESCE(ethnicity_co_applicant_1, ''),
      COALESCE(ethnicity_co_applicant_2, ''),
      COALESCE(ethnicity_co_applicant_3, ''),
      COALESCE(ethnicity_co_applicant_4, ''),
      COALESCE(ethnicity_co_applicant_5, ''),
      COALESCE(CAST(ethnicity_observed_co_applicant as varchar), ''),
      COALESCE(race_applicant_1, ''),
      COALESCE(race_applicant_2, ''),
      COALESCE(race_applicant_3, ''),
      COALESCE(race_applicant_4, ''),
      COALESCE(race_applicant_5, ''),
      COALESCE(race_co_applicant_1, ''),
      COALESCE(race_co_applicant_2, ''),
      COALESCE(race_co_applicant_3, ''),
      COALESCE(race_co_applicant_4, ''),
      COALESCE(race_co_applicant_5, ''),
      COALESCE(CAST(race_observed_applicant as varchar), ''),
      COALESCE(CAST(race_observed_co_applicant as varchar), ''),
      sex_applicant,
      sex_co_applicant,
      observed_sex_applicant,
      observed_sex_co_applicant,
      age_applicant,
      applicant_age_greater_than_62,
      age_co_applicant,
      coapplicant_age_greater_than_62,
      income,
      purchaser_type,
      rate_spread,
      hoepa_status,
      lien_status,
      credit_score_type_applicant,
      credit_score_type_co_applicant,
      COALESCE(CAST(denial_reason1 as varchar), ''),
      COALESCE(CAST(denial_reason2 as varchar), ''),
      COALESCE(CAST(denial_reason3 as varchar), ''),
      COALESCE(CAST(denial_reason4 as varchar), ''),
      total_loan_costs,
      total_points,
      origination_charges,
      COALESCE(discount_points, ''),
      COALESCE(lender_credits, ''),
      interest_rate,
      payment_penalty,
      debt_to_incode,
      loan_value_ratio,
      loan_term,
      rate_spread_intro,
      baloon_payment,
      insert_only_payment,
      amortization,
      other_amortization,
      property_value,
      home_security_policy,
      lan_property_interest,
      total_units,
      mf_affordable,
      application_submission,
      payable,
      COALESCE(CAST(aus1 as varchar), ''),
      COALESCE(CAST(aus2 as varchar), ''),
      COALESCE(CAST(aus3 as varchar), ''),
      COALESCE(CAST(aus4 as varchar), ''),
      COALESCE(CAST(aus5 as varchar), ''),
      reverse_mortgage,
      line_of_credits,
      business_or_commercial,
      population,
      minority_population_percent,
      ffiec_med_fam_income,
      tract_to_msamd,
      owner_occupied_units,
      one_to_four_fam_units,
      msa_md,
      COALESCE(loan_flag, ''),
      created_at,
      submission_id,
      msa_md_name,
      filing_year,
      conforming_loan_limit,
      median_age,
      median_age_calculated,
      median_income_percentage,
      race_categorization,
      sex_categorization,
      ethnicity_categorization,
      percent_median_msa_income
      """.stripMargin

  def escape(str: String): String = str.replace("'", "")

  def formatSeq(strs: Seq[String]): String =
    strs.map(each => s"\'$each\'").mkString(start = "(", sep = ",", end = ")")

  def eq(fieldName: String, value: String): String =
    s"${escape(fieldName)} = '${escape(value)}'"

  def in(fieldName: String, values: Seq[String]): String =
    s"${escape(fieldName)} IN ${formatSeq(values.map(escape))}"

  def whereAndOpt(expression: String, remainingExpressions: String*): String = {
    val primary = s"WHERE $expression"
    if (remainingExpressions.isEmpty) primary
    else {
      val secondaries =
        remainingExpressions.map(expr => s"AND $expr").mkString(sep = " ")
      s"$primary $secondaries"
    }
  }

  override def find(
      browserFields: List[QueryField]): Source[ModifiedLarEntity, NotUsed] = {
    val queries = browserFields.map(field => in(field.dbName, field.values))

    val filterCriteria = queries match {
      case Nil          => ""
      case head :: tail => whereAndOpt(head, tail: _*)
    }

    val searchQuery = sql"""
      SELECT #${columns}
      FROM #${tableName}
      #$filterCriteria
      """
      .as[ModifiedLarEntity]
      .withStatementParameters(
        rsType = ResultSetType.ForwardOnly,
        rsConcurrency = ResultSetConcurrency.ReadOnly,
        fetchSize = 1000
      )
      .transactionally

    val publisher = db.stream(searchQuery)
    Source.fromPublisher(publisher)
  }

  override def findAndAggregate(
      browserFields: List[QueryField]): Task[Statistic] = {
    val queries = browserFields.map(field => in(field.dbName, field.values))
    val filterCriteria = queries match {
      case Nil          => ""
      case head :: tail => whereAndOpt(head, tail: _*)
    }
    val query = sql"""
        SELECT
          COUNT(loan_amount),
          SUM(loan_amount)
        FROM #${tableName}
        #$filterCriteria
        """.as[Statistic].head

    Task.deferFuture(db.run(query)).guarantee(Task.shift)
  }
}
