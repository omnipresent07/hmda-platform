package hmda.publication.lar

import java.io.File
import java.nio.file.Paths
import hmda.publication.lar.LarConverter

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.util.ByteString
import hmda.messages.submission.HmdaRawDataEvents.LineAdded
import hmda.messages.submission.SubmissionProcessingCommands.{
  PersistHmdaRowParsedError,
  StartParsing
}
import hmda.messages.submission.SubmissionProcessingEvents.HmdaRowParsedError
import hmda.model.filing.lar.LoanApplicationRegister
import hmda.parser.filing.lar.LarCsvParser
import hmda.query.HmdaQuery.eventsByPersistenceId
import hmda.util.streams.FlowUtils.framing

import scala.util.{Failure, Success}

object IrsDebug extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //runMain hmda.publication.lar.IrsDebug
//  val persistenceId = s"HmdaRawData-254900TJCZAM25V2WZ38-2018-9"
//  val persistenceId = s"HmdaRawData-549300JYXTZDSPJEPI44-2018-4"
//  val persistenceId = s"HmdaRawData-2549006DVF3CPHSCHN79-2018-7"
//  val persistenceId = s"HmdaRawData-549300XR0EY1M0FVG232-2018-1"
//  val persistenceId = s"HmdaRawData-549300ZIQ24V0C88AC41-2018-3"
//  val persistenceId = s"HmdaParserError-549300KNV94E4Y2HSK54-2019-1"
  val persistenceId = s"HmdaRawData-5493004O9GGH58IPFT94-2019-3"
  val source: Source[LineAdded, NotUsed] = eventsByPersistenceId(persistenceId)
    .collect {
      case evt: LineAdded => evt
    }

//  val source: Source[HmdaRowParsedError, NotUsed] =
//    eventsByPersistenceId(persistenceId)
//      .collect {
//        case evt: HmdaRowParsedError => evt
//      }
  val file = Paths.get("5493004O9GGH58IPFT94-withparser.txt")
  source
    .drop(1)
    .map(l => l.data)
    .map(ByteString(_))
    .via(framing("\n"))
    .map(_.utf8String)
    .map(_.trim)
    .map(s =>
      LarCsvParser(s)
        .getOrElse(LoanApplicationRegister()))
    .filter(lar => lar.larIdentifier.LEI != "")
    .filter(_.loan.ULI == "5493004O9GGH58IPFT94W121761")
    .map(lar => LarConverter(lar))
//    .runWith(Sink.foreach(println))
//    .map(t => ByteString(t))
//    .via(framing("\n"))
//    .runWith(FileIO.toPath(file))
    .runWith(Sink.foreach(println))
//    .onComplete(_ => system.terminate())(
//      scala.concurrent.ExecutionContext.global)
    .onComplete {
      case (Success(_))  => system.terminate()
      case (Failure(ex)) => println(s"Error happened: ${ex}")
    }(scala.concurrent.ExecutionContext.global)
//  FileIO.toPath(file)
}
