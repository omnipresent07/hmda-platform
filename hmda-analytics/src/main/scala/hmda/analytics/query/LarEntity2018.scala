package hmda.analytics.query

case class LarEntity2018(
    id: Int = 0,
    lei: String = "",
    uli: String = "",
    appDate: String = "",
    loanType: Int = 0,
    loanPurpose: Int = 0,
    preapproval: Int = 0,
    constructionMethod: Int = 0,
    occupancyType: Int = 0,
    loanAmount: BigDecimal = 0.0,
    actionTakenType: Int = 0,
    actionTakenDate: Int = 0,
    street: String = "",
    city: String = "",
    state: String = "",
    zip: String = "",
    county: String = "",
    tract: String = "",
    ethnicityApplicant1: String = "",
    ethnicityApplicant2: String = "",
    ethnicityApplicant3: String = "",
    ethnicityApplicant4: String = "",
    ethnicityApplicant5: String = "",
    otherHispanicApplicant: String = "",
    ethnicityCoApplicant1: String = "",
    ethnicityCoApplicant2: String = "",
    ethnicityCoApplicant3: String = "",
    ethnicityCoApplicant4: String = "",
    ethnicityCoApplicant5: String = "",
    otherHispanicCoApplicant: String = "",
    ethnicityObservedApplicant: Int = 0,
    ethnicityObservedCoApplicant: Int = 0,
    raceApplicant1: String = "",
    raceApplicant2: String = "",
    raceApplicant3: String = "",
    raceApplicant4: String = "",
    raceApplicant5: String = "",
    otherNativeRaceApplicant: String = "",
    otherAsianRaceApplicant: String = "",
    otherPacificRaceApplicant: String = "",
    rateCoApplicant1: String = "",
    rateCoApplicant2: String = "",
    rateCoApplicant3: String = "",
    rateCoApplicant4: String = "",
    rateCoApplicant5: String = "",
    otherNativeRaceCoApplicant: String = "",
    otherAsianRaceCoApplicant: String = "",
    otherPacificRaceCoApplicant: String = "",
    raceObservedApplicant: Int = 0,
    raceObservedCoApplicant: Int = 0,
    sexApplicant: Int = 0,
    sexCoApplicant: Int = 0,
    observedSexApplicant: Int = 0,
    observedSexCoApplicant: Int = 0,
    ageApplicant: Int = 0,
    ageCoApplicant: Int = 0,
    income: String = "",
    purchaserType: Int = 0,
    rateSpread: String = "",
    hoepaStatus: Int = 0,
    lienStatus: Int = 0,
    creditScoreApplicant: Int = 0,
    creditScoreCoApplicant: Int = 0,
    creditScoreTypeApplicant: Int = 0,
    creditScoreModelApplicant: String = "",
    creditScoreTypeCoApplicant: Int = 0,
    creditScoreModelCoApplicant: String = "",
    denialReason1: String = "",
    denialReason2: String = "",
    denialReason3: String = "",
    denialReason4: String = "",
    otherDenialReason: String = "",
    totalLoanCosts: String = "",
    totalPoints: String = "",
    originationCharges: String = "",
    discountPoints: String = "",
    lenderCredits: String = "",
    interestRate: String = "",
    paymentPenalty: String = "",
    debtToIncome: String = "",
    loanValueRatio: String = "",
    loanTerm: String = "",
    rateSpreadIntro: String = "",
    baloonPayment: Int = 0,
    insertOnlyPayment: Int = 0,
    amortization: Int = 0,
    otherAmortization: Int = 0,
    propertyValues: String = "",
    homeSecurityPolicy: Int = 0,
    landPropertyInterest: Int = 0,
    totalUnits: Int = 0,
    mfAffordable: String = "",
    applicationSubmission: Int = 0,
    payable: Int = 0,
    nmls: String = "",
    aus1: String = "",
    aus2: String = "",
    aus3: String = "",
    aus4: String = "",
    aus5: String = "",
    otheraus: String = "",
    aus1Result: Int = 0,
    aus2Result: String = "",
    aus3Result: String = "",
    aus4Result: String = "",
    aus5Result: String = "",
    otherAusResult: String = "",
    reverseMortgage: Int = 0,
    lineOfCredits: Int = 0,
    businessOrCommercial: Int = 0,
    conformingLoanLimit: String = "",
    ethnicityCategorization: String = "",
    raceCategorization: String = "",
    sexCategorization: String = "",
    dwellingCategorization: String = "",
    loanProductTypeCategorization: String = "",
    tractPopulation: Int = 0,
    tractMinorityPopulationPercent: Double = 0.0,
    tractMedianIncome: Int = 0,
    tractOccupiedUnits: Int = 0,
    tractOneToFourFamilyUnits: Int = 0,
    tractMedianAge: Int = 0,
    tractToMsaIncomePercent: Double = 0.0,
    isQuarterly: Boolean = false,
    msa_md: String,
    msa_md_name: String
) {
  def isEmpty: Boolean = lei == ""
}
