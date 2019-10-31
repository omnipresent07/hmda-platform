package hmda.util

import hmda.model.filing.submission.SubmissionId
import org.slf4j.LoggerFactory
import hmda.utils._

object BankFilterUtils {
  val log = LoggerFactory.getLogger("hmda")

  def filterBankWithLogging(institutionLei: String, bankIgnoreList: Array[String]): Boolean =
    if (bankIgnoreList.contains(institutionLei.toUpperCase)) {
      log.info(s"Filtered out $institutionLei.toUpperCase")
      false
    } else true

  def filterQuarterlyFiling(submissionId: SubmissionId): Boolean =
    YearUtils.isQuarterlyFiling(submissionId)
}
