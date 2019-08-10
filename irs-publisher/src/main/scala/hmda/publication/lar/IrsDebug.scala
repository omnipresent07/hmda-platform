package hmda.publication.lar

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import hmda.messages.submission.HmdaRawDataEvents.LineAdded
import hmda.model.filing.lar.LoanApplicationRegister
import hmda.parser.filing.lar.LarCsvParser
import hmda.query.HmdaQuery.eventsByPersistenceId
import hmda.util.streams.FlowUtils.framing

object IrsDebug extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  val persistenceId = s"HmdaRawData-254900TJCZAM25V2WZ38-2018-9"
//  val persistenceId = s"HmdaRawData-549300JYXTZDSPJEPI44-2018-4"

  val source: Source[LineAdded, NotUsed] = eventsByPersistenceId(persistenceId)
    .collect {
      case evt: LineAdded => evt
    }

  source
    .drop(1)
    .map(l => l.data)
    .map(ByteString(_))
    .via(framing("\n"))
    .map(_.utf8String)
    .map(_.trim)
    .map(s => LarCsvParser(s, true).getOrElse(LoanApplicationRegister()))
    .runWith(Sink.foreach(println))
    .onComplete(_ => system.terminate())(
      scala.concurrent.ExecutionContext.global)

}
