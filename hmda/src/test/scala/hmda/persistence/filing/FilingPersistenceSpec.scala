package hmda.persistence.filing

import akka.actor
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.adapter._
import hmda.messages.filing.FilingCommands.{
  AddSubmission,
  GetLatestSubmission,
  GetSubmissions
}
import hmda.messages.filing.FilingEvents.FilingEvent
import hmda.model.filing.Filing
import hmda.persistence.AkkaCassandraPersistenceSpec
import hmda.model.filing.FilingGenerator._
import hmda.model.filing.submission.{Submission, SubmissionId}
import hmda.model.submission.SubmissionGenerator._

class FilingPersistenceSpec extends AkkaCassandraPersistenceSpec {
  override implicit val system = actor.ActorSystem()
  override implicit val typedSystem = system.toTyped

  val filingProbe = TestProbe[FilingEvent]("filing-event-probe")
  val maybeSubmissionProbe =
    TestProbe[Option[Submission]]("maybe-submission-probe")
  val submissionProbe = TestProbe[Submission]("submission-probe")
  val submissionsProbe = TestProbe[List[Submission]](name = "submissions-probe")

  val sampleFiling = filingGen
    .suchThat(_.lei != "")
    .suchThat(_.period != "")
    .sample
    .getOrElse(Filing())

  val sampleSubmission = submissionGen
    .suchThat(s => !s.id.isEmpty)
    .suchThat(s => s.id.lei != "" && s.id.lei != "AA")
    .sample
    .getOrElse(Submission(SubmissionId("12345", "2018", 1)))

  "Filings" must {
    "be created and read back" in {
      val filingPersistence = system.spawn(
        FilingPersistence.behavior(sampleFiling.lei, sampleFiling.period),
        actorName)

      filingPersistence ! GetLatestSubmission(maybeSubmissionProbe.ref)
      maybeSubmissionProbe.expectMessage(None)

      filingPersistence ! AddSubmission(sampleSubmission, submissionProbe.ref)
      submissionProbe.expectMessage(sampleSubmission)

      filingPersistence ! GetLatestSubmission(maybeSubmissionProbe.ref)
      maybeSubmissionProbe.expectMessage(Some(sampleSubmission))

      val sampleSubmission2 =
        sampleSubmission.copy(id = sampleSubmission.id.copy(lei = "AAA"))
      filingPersistence ! AddSubmission(sampleSubmission2, submissionProbe.ref)
      submissionProbe.expectMessage(sampleSubmission2)

      filingPersistence ! GetSubmissions(submissionsProbe.ref)
      submissionsProbe.expectMessage(List(sampleSubmission2, sampleSubmission))

      filingPersistence ! GetLatestSubmission(maybeSubmissionProbe.ref)
      maybeSubmissionProbe.expectMessage(Some(sampleSubmission2))
    }
  }
}
