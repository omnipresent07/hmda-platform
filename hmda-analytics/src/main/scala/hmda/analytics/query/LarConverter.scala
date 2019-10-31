package hmda.analytics.query

import hmda.model.filing.lar.LoanApplicationRegister
import hmda.parser.derivedFields._
import hmda.model.census.CountyLoanLimit
import hmda.census.records._
import hmda.model.census.Census
import com.typesafe.config.ConfigFactory
import hmda.utils.YearUtils.Period

object LarConverter {

  val config = ConfigFactory.load()

  val censusFileName2019 =
    config.getString("hmda.census.fields.2019.filename")

  val censusTractMap: Map[String, Census] =
    CensusRecords.indexedTract2019

  val censusRecords = CensusRecords.parseCensusFile(censusFileName2019)
  val countyLoanLimits: Seq[CountyLoanLimit] =
    CountyLoanLimitRecords.parseCountyLoanLimitFile()
  val countyLoanLimitsByCounty: Map[String, CountyLoanLimit] =
    countyLoanLimits
      .map(county => county.stateCode + county.countyCode -> county)
      .toMap
  val countyLoanLimitsByState: Map[String, StateBoundries] =
    countyLoanLimits.groupBy(county => county.stateAbbrv).mapValues { countyList =>
      val oneUnit   = countyList.map(county => county.oneUnitLimit)
      val twoUnit   = countyList.map(county => county.twoUnitLimit)
      val threeUnit = countyList.map(county => county.threeUnitLimit)
      val fourUnit  = countyList.map(county => county.fourUnitLimit)
      StateBoundries(
        oneUnitMax = oneUnit.max,
        oneUnitMin = oneUnit.min,
        twoUnitMax = twoUnit.max,
        twoUnitMin = twoUnit.min,
        threeUnitMax = threeUnit.max,
        threeUnitMin = threeUnit.min,
        fourUnitMax = fourUnit.max,
        fourUnitMin = fourUnit.min
      )
    }

  def apply(
    lar: LoanApplicationRegister,
    isQuarterly: Boolean = false
  ): LarEntity = {
    val census = censusTractMap.getOrElse(lar.geography.tract, Census())
    LarEntity(
      lar.larIdentifier.id,
      lar.larIdentifier.LEI,
      lar.loan.ULI,
      lar.loan.applicationDate,
      lar.loan.loanType.code,
      lar.loan.loanPurpose.code,
      lar.action.preapproval.code,
      lar.loan.constructionMethod.code,
      lar.loan.occupancy.code,
      lar.loan.amount,
      lar.action.actionTakenType.code,
      lar.action.actionTakenDate,
      lar.geography.street,
      lar.geography.city,
      lar.geography.state,
      lar.geography.zipCode,
      lar.geography.county,
      lar.geography.tract,
      convertEmptyField(lar.applicant.ethnicity.ethnicity1.code),
      convertEmptyField(lar.applicant.ethnicity.ethnicity2.code),
      convertEmptyField(lar.applicant.ethnicity.ethnicity3.code),
      convertEmptyField(lar.applicant.ethnicity.ethnicity4.code),
      convertEmptyField(lar.applicant.ethnicity.ethnicity5.code),
      lar.applicant.ethnicity.otherHispanicOrLatino,
      convertEmptyField(lar.coApplicant.ethnicity.ethnicity1.code),
      convertEmptyField(lar.coApplicant.ethnicity.ethnicity2.code),
      convertEmptyField(lar.coApplicant.ethnicity.ethnicity3.code),
      convertEmptyField(lar.coApplicant.ethnicity.ethnicity4.code),
      convertEmptyField(lar.coApplicant.ethnicity.ethnicity5.code),
      lar.coApplicant.ethnicity.otherHispanicOrLatino,
      lar.applicant.ethnicity.ethnicityObserved.code,
      lar.coApplicant.ethnicity.ethnicityObserved.code,
      convertEmptyField(lar.applicant.race.race1.code),
      convertEmptyField(lar.applicant.race.race2.code),
      convertEmptyField(lar.applicant.race.race3.code),
      convertEmptyField(lar.applicant.race.race4.code),
      convertEmptyField(lar.applicant.race.race5.code),
      lar.applicant.race.otherNativeRace,
      lar.applicant.race.otherAsianRace,
      lar.applicant.race.otherPacificIslanderRace,
      convertEmptyField(lar.coApplicant.race.race1.code),
      convertEmptyField(lar.coApplicant.race.race2.code),
      convertEmptyField(lar.coApplicant.race.race3.code),
      convertEmptyField(lar.coApplicant.race.race4.code),
      convertEmptyField(lar.coApplicant.race.race5.code),
      lar.coApplicant.race.otherNativeRace,
      lar.coApplicant.race.otherAsianRace,
      lar.coApplicant.race.otherPacificIslanderRace,
      lar.applicant.race.raceObserved.code,
      lar.coApplicant.race.raceObserved.code,
      lar.applicant.sex.sexEnum.code,
      lar.coApplicant.sex.sexEnum.code,
      lar.applicant.sex.sexObservedEnum.code,
      lar.coApplicant.sex.sexObservedEnum.code,
      lar.applicant.age,
      lar.coApplicant.age,
      lar.income,
      lar.purchaserType.code,
      lar.loan.rateSpread,
      lar.hoepaStatus.code,
      lar.lienStatus.code,
      lar.applicant.creditScore,
      lar.coApplicant.creditScore,
      lar.applicant.creditScoreType.code,
      lar.applicant.otherCreditScoreModel,
      lar.coApplicant.creditScoreType.code,
      lar.coApplicant.otherCreditScoreModel,
      convertEmptyField(lar.denial.denialReason1.code),
      convertEmptyField(lar.denial.denialReason2.code),
      convertEmptyField(lar.denial.denialReason3.code),
      convertEmptyField(lar.denial.denialReason4.code),
      lar.denial.otherDenialReason,
      lar.loanDisclosure.totalLoanCosts,
      lar.loanDisclosure.totalPointsAndFees,
      lar.loanDisclosure.originationCharges,
      lar.loanDisclosure.discountPoints,
      lar.loanDisclosure.lenderCredits,
      lar.loan.interestRate,
      lar.loan.prepaymentPenaltyTerm,
      lar.loan.debtToIncomeRatio,
      lar.loan.combinedLoanToValueRatio,
      lar.loan.loanTerm,
      lar.loan.introductoryRatePeriod,
      lar.nonAmortizingFeatures.balloonPayment.code,
      lar.nonAmortizingFeatures.interestOnlyPayments.code,
      lar.nonAmortizingFeatures.negativeAmortization.code,
      lar.nonAmortizingFeatures.otherNonAmortizingFeatures.code,
      lar.property.propertyValue,
      lar.property.manufacturedHomeSecuredProperty.code,
      lar.property.manufacturedHomeLandPropertyInterest.code,
      lar.property.totalUnits,
      lar.property.multiFamilyAffordableUnits,
      lar.applicationSubmission.code,
      lar.payableToInstitution.code,
      lar.larIdentifier.NMLSRIdentifier,
      convertEmptyField(lar.AUS.aus1.code),
      convertEmptyField(lar.AUS.aus2.code),
      convertEmptyField(lar.AUS.aus3.code),
      convertEmptyField(lar.AUS.aus4.code),
      convertEmptyField(lar.AUS.aus5.code),
      lar.AUS.otherAUS,
      lar.ausResult.ausResult1.code,
      convertEmptyField(lar.ausResult.ausResult2.code),
      convertEmptyField(lar.ausResult.ausResult3.code),
      convertEmptyField(lar.ausResult.ausResult4.code),
      convertEmptyField(lar.ausResult.ausResult5.code),
      lar.ausResult.otherAusResult,
      lar.reverseMortgage.code,
      lar.lineOfCredit.code,
      lar.businessOrCommercialPurpose.code,
      ConformingLoanLimit.assignLoanLimit(lar, countyLoanLimitsByCounty, countyLoanLimitsByState),
      EthnicityCategorization.assignEthnicityCategorization(lar),
      RaceCategorization.assignRaceCategorization(lar),
      SexCategorization.assignSexCategorization(lar),
      DwellingCategorization.assignDwellingCategorization(lar),
      LoanProductTypeCategorization.assignLoanProductTypeCategorization(lar),
      census.population,
      census.minorityPopulationPercent,
      census.medianIncome,
      census.occupiedUnits,
      census.oneToFourFamilyUnits,
      census.medianAge,
      census.tracttoMsaIncomePercent,
      isQuarterly
    )
  }

  private def convertEmptyField(code: Int) =
    if (code == 0) "" else code.toString

}
