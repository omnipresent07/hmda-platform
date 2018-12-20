package hmda.analytics.query

import hmda.query.DbConfiguration.dbConfig
import hmda.query.repository.TableRepository
import slickless._
import shapeless._
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

import scala.concurrent.Future

trait LarComponent {

  import dbConfig.profile.api._

  class LarTable(tag: Tag)
      extends Table[LarEntity](tag, "loanapplicationregister2018") {

    def id = column[Int]("id")
    def lei = column[String]("lei")
    def uli = column[String]("uli")
    def appDate = column[String]("application_date")
    def loanType = column[Int]("loan_type")
    def loanPurpose = column[Int]("loan_purpose")
    def preapproval = column[Int]("preapproval")
    def constructionMethod = column[Int]("construction_method")
    def occupancyType = column[Int]("occupancy_type")
    def loanAmount = column[Double]("loan_amount")
    def actionTakenType = column[Int]("action_taken_type")
    def actionTakenDate = column[Int]("action_taken_date")
    def street = column[String]("street")
    def city = column[String]("city")
    def state = column[String]("state")
    def zip = column[String]("zip")
    def county = column[String]("county")
    def tract = column[String]("tract")
    def ethnicityApplicant1 = column[Int]("ethnicity_applicant_1")
    def ethnicityApplicant2 = column[Int]("ethnicity_applicant_2")
    def ethnicityApplicant3 = column[Int]("ethnicity_applicant_3")
    def ethnicityApplicant4 = column[Int]("ethnicity_applicant_4")
    def ethnicityApplicant5 = column[Int]("ethnicity_applicant_5")
    def otherHispanicApplicant = column[String]("other_hispanic_applicant") //24
    def ethnicityCoApplicant1 = column[Int]("ethnicity_co_applicant_1")
    def ethnicityCoApplicant2 = column[Int]("ethnicity_co_applicant_2")
    def ethnicityCoApplicant3 = column[Int]("ethnicity_co_applicant_3")
    def ethnicityCoApplicant4 = column[Int]("ethnicity_co_applicant_4")
    def ethnicityCoApplicant5 = column[Int]("ethnicity_co_applicant_5")
    def otherHispanicCoApplicant = column[String]("other_hispanic_co_applicant")
    def ethnicityObservedApplicant = column[Int]("ethnicity_observed_applicant")
    def ethnicityObservedCoApplicant =
      column[Int]("ethnicity_observed_co_applicant")
    def raceApplicant1 = column[Int]("race_applicant_1")
    def raceApplicant2 = column[Int]("race_applicant_2")
    def raceApplicant3 = column[Int]("race_applicant_3")
    def raceApplicant4 = column[Int]("race_applicant_4")
    def raceApplicant5 = column[Int]("race_applicant_5")
    def otherNativeRaceApplicant = column[String]("other_native_race_applicant")
    def otherAsicanRaceApplicant = column[String]("other_asian_race_applicant")
    def otherPacticifRaceApplicant =
      column[String]("other_pacific_race_applicant")
    def rateCoApplicant1 = column[Int]("race_co_applicant_1")
    def rateCoApplicant2 = column[Int]("race_co_applicant_2")
    def rateCoApplicant3 = column[Int]("race_co_applicant_3")
    def rateCoApplicant4 = column[Int]("race_co_applicant_4")
    def rateCoApplicant5 = column[Int]("race_co_applicant_5")
    def otherNaticeRaceCoApplicant =
      column[String]("other_native_race_co_applicant")
    def otherAsianRaceCoApplicant =
      column[String]("other_asian_race_co_applicant")
    def otherPacificRaceCoApplicant =
      column[String]("other_pacific_race_co_applicant")
    def raceObservedApplicant = column[Int]("race_observed_applicant")
    def raceObservedCoApplicant = column[Int]("race_observed_co_applicant")
    def sexApplicant = column[Int]("sex_applicant")
    def sexCoApplicant = column[Int]("sex_co_applicant")
    def observedSexApplicant = column[Int]("observed_sex_applicant")
    def observedSexCoApplicant = column[Int]("observed_sex_co_applicant")
    def ageApplicant = column[Int]("age_applicant")
    def ageCoApplicant = column[Int]("age_co_applicant")
    def income = column[String]("income")
    def purchaserType = column[Int]("purchaser_type")
    def rateSpread = column[String]("rate_spread")
    def hoepaStatus = column[Int]("hoepa_status")
    def lienStatus = column[Int]("lien_status")
    def creditScoreApplicant = column[Int]("credit_score_applicant")
    def creditScoreCoApplicant = column[Int]("credit_score_co_applicant")
    def creditScoreTypeApplicant = column[Int]("credit_score_type_applicant")
    def creditScoreModelApplicant =
      column[String]("credit_score_model_applicant")
    def creditScoreTypeCoApplicant =
      column[Int]("credit_score_type_co_applicant")
    def creditScoreModelCoApplicant =
      column[String]("credit_score_model_co_applicant")
    def denialReason1 = column[Int]("denial_reason1")
    def denialReason2 = column[Int]("denial_reason2")
    def denialReason3 = column[Int]("denial_reason3")
    def denialReason4 = column[Int]("denial_reason4") //71
    def otherDenialReason = column[String]("other_denial_reason")
    def totalLoanCosts = column[String]("total_loan_costs")
    def totalPoints = column[String]("total_points")
//    def someelse = column[String]("origination_charges")
    def discountPoints = column[String]("discount_points")
//    def lenderCredits = column[String]("lender_credits")
    def interestRate = column[String]("interest_rate")
//    def paymentPenalty = column[String]("payment_penalty")
//    def debtToIncome = column[String]("debt_to_incode") //80
//    def loanValueRatio = column[String]("loan_value_ratio")
//    def loanTerm = column[String]("loan_term")
//    def rateSpreadIntro = column[String]("rate_spread_intro")
//    def baloonPayment = column[Int]("baloon_payment")
//    def insertOnlyPayment = column[Int]("insert_only_payment")
//    def amortization = column[Int]("amortization") //86
//    def otherAmortization = column[Int]("other_amortization")
//    def propertyValues = column[String]("proprerty_valeue")
//    def homeSecurityPolicy = column[Int]("home_security_policy")
//    def landPropertyInterest = column[Int]("lan_property_interest") //90
//    def totalUnits = column[Int]("total_uits")
//    def mfAffordable = column[String]("mf_affordable")
//    def applicationSubmission = column[String]("application_submission")
//    def payable = column[Int]("payable")
//    def nmls = column[Int]("nmls")
//    def aus1 = column[Int]("aus1")
//    def aus2 = column[Int]("aus2")
//    def aus3 = column[Int]("aus3")
//    def aus4 = column[Int]("aus4")
//    def aus5 = column[Int]("aus5")
//    def otheraus = column[Int]("other_aus")
//    def aus1Result = column[Int]("aus1_result")
//    def aus2Result = column[Int]("aus2_result")
//    def aus3Result = column[Int]("aus3_result")
//    def aus4Result = column[Int]("aus4_result")
//    def aus5Result = column[Int]("aus5_result")
//    def otherAusResult = column[String]("other_aus_result")
//    def reverseMortgage = column[Int]("reverse_mortgage")
//    def lineOfCredits = column[Int]("line_of_credits")
//    def businessOrCommercial = column[Int]("business_or_commercial")

    def * =
      (
        id :: lei
          :: uli
          :: appDate
          :: loanType
          :: loanPurpose
          :: preapproval :: constructionMethod
          :: occupancyType :: loanAmount
          :: actionTakenType :: actionTakenDate :: street :: city :: state :: zip :: county :: tract ::
          ethnicityApplicant1 :: ethnicityApplicant2
          :: ethnicityApplicant3 :: ethnicityApplicant4 :: ethnicityApplicant5 ::
          otherHispanicApplicant ::
          ethnicityCoApplicant1 ::
          ethnicityCoApplicant2 ::
          ethnicityCoApplicant3 ::
          ethnicityCoApplicant4 ::
          ethnicityCoApplicant5 ::
          otherHispanicCoApplicant ::
          ethnicityObservedApplicant ::
          ethnicityObservedCoApplicant ::
          raceApplicant1 ::
          raceApplicant2 ::
          raceApplicant3 ::
          raceApplicant4 ::
          raceApplicant5 ::
          otherNativeRaceApplicant ::
          otherAsicanRaceApplicant ::
          otherPacticifRaceApplicant ::
          rateCoApplicant1 ::
          rateCoApplicant2 ::
          rateCoApplicant3 ::
          rateCoApplicant4 ::
          rateCoApplicant5 ::
          otherNaticeRaceCoApplicant ::
          otherAsianRaceCoApplicant ::
          otherPacificRaceCoApplicant ::
          raceObservedApplicant ::
          raceObservedCoApplicant ::
          sexApplicant ::
          sexCoApplicant ::
          observedSexApplicant ::
          observedSexCoApplicant ::
          ageApplicant ::
          ageCoApplicant ::
          income ::
          purchaserType ::
          rateSpread ::
          hoepaStatus ::
          lienStatus ::
          creditScoreApplicant ::
          creditScoreCoApplicant ::
          creditScoreTypeApplicant ::
          creditScoreModelApplicant ::
          creditScoreTypeCoApplicant ::
          creditScoreModelCoApplicant ::
          denialReason1 ::
          denialReason2 ::
          denialReason3 ::
          denialReason4 :: //71
          otherDenialReason ::
          totalLoanCosts ::
          totalPoints ::
//          someelse ::
          discountPoints ::
//          lenderCredits ::
          interestRate ::
//          paymentPenalty ::
//          debtToIncome ::
//          loanValueRatio ::
//          loanTerm ::
//          rateSpreadIntro ::
//          baloonPayment ::
//          insertOnlyPayment ::
//          amortization ::
//          otherAmortization ::
//          propertyValues ::
//          homeSecurityPolicy ::
//          landPropertyInterest ::
//          totalUnits ::
//          mfAffordable ::
//          applicationSubmission ::
//          payable ::
//          nmls ::
//          aus1 ::
//          aus2 ::
//          aus3 ::
//          aus4 ::
//          aus5 ::
//          otheraus ::
//          aus1Result ::
//          aus2Result ::
//          aus3Result ::
//          aus4Result ::
//          aus5Result ::
//          otherAusResult ::
//          reverseMortgage ::
//          lineOfCredits ::
//          businessOrCommercial
//          ::
          HNil
      ).mappedWith(Generic[LarEntity])
  }

//  val larTable = TableQuery[LarTable]
//
//  class LarRepository(val config: DatabaseConfig[JdbcProfile])
//      extends TableRepository[LarTable, String] {
//
//    override val table: config.profile.api.TableQuery[LarTable] = larTable
//
//    override def getId(row: LarTable): config.profile.api.Rep[Id] =
//      row.lei
//
//    def insert(lar: LarEntity): Future[Int] = {
//      db.run(table += lar)
//    }
//
//    def deleteByLei(lei: String): Future[Int] = {
//      db.run(table.filter(_.lei === lei).delete)
//    }
//
//  }

//  }

}
