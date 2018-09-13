package hmda.validation.rules.lar.quality

import hmda.model.filing.lar.LoanApplicationRegister
import hmda.model.filing.lar.enums._
import hmda.validation.dsl.PredicateCommon._
import hmda.validation.dsl.PredicateSyntax._
import hmda.validation.dsl.ValidationResult
import hmda.validation.rules.EditCheck

object Q631 extends EditCheck[LoanApplicationRegister] {
  override def name: String = "Q631"

  override def apply(lar: LoanApplicationRegister): ValidationResult = {
    when(lar.loan.loanType is oneOf(FHAInsured,
                                    VAGuaranteed,
                                    RHSOrFSAGuaranteed)) {
      lar.property.totalUnits is lessThanOrEqual(4)
    }
  }
}
