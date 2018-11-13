package hmda.validation.filing

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL}
import akka.util.ByteString
import hmda.model.filing.PipeDelimited
import hmda.model.filing.lar.LoanApplicationRegister
import hmda.model.filing.ts.TransmittalSheet
import hmda.parser.filing.lar.LarCsvParser
import hmda.parser.filing.ts.TsCsvParser
import hmda.validation.HmdaValidated
import hmda.validation.context.ValidationContext
import hmda.util.streams.FlowUtils._
import hmda.validation.engine.LarEngine
import hmda.validation.engine.TsEngine

object ValidationFlow {

  def validateHmdaFile(checkType: String, ctx: ValidationContext)
    : Flow[ByteString, HmdaValidated[PipeDelimited], NotUsed] = {
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[ByteString](2))
      val concat = b.add(Concat[HmdaValidated[PipeDelimited]](2))

      bcast.take(1) ~> validateTsFlow(checkType, ctx) ~> concat.in(0)
      bcast.drop(1) ~> validateLarFlow(checkType, ctx) ~> concat.in(1)

      FlowShape(bcast.in, concat.out)
    })
  }

  def validateTsFlow(checkType: String, ctx: ValidationContext)
    : Flow[ByteString, HmdaValidated[TransmittalSheet], NotUsed] = {
    Flow[ByteString]
      .via(framing("\n"))
      .map(_.utf8String)
      .map(_.trim)
      .map(s => TsCsvParser(s))
      .collect {
        case Right(ts) => ts
      }
      .map { ts =>
        checkType match {
          case "all"         => TsEngine.checkAll(ts, ts.LEI, ctx)
          case "syntactical" => TsEngine.checkSyntactical(ts, ts.LEI, ctx)
          case "validity"    => TsEngine.checkValidity(ts, ts.LEI)
        }
      }
      .map { x =>
        x.leftMap(xs => xs.toList).toEither
      }
  }

  def validateLarFlow(checkType: String, ctx: ValidationContext)
    : Flow[ByteString, HmdaValidated[LoanApplicationRegister], NotUsed] = {
    Flow[ByteString]
      .via(framing("\n"))
      .map(_.utf8String)
      .map(_.trim)
      .map(s => LarCsvParser(s))
      .collect {
        case Right(lar) => lar
      }
      .map { lar =>
        checkType match {
          case "all" => LarEngine.checkAll(lar, lar.loan.ULI, ctx)
          case "syntactical" =>
            LarEngine.checkSyntactical(lar, lar.loan.ULI, ctx)
          case "validity" => LarEngine.checkValidity(lar, lar.loan.ULI)
          case "quality"  => LarEngine.checkQuality(lar, lar.loan.ULI)
        }
      }
      .map { x =>
        x.leftMap(xs => xs.toList).toEither
      }
  }
}