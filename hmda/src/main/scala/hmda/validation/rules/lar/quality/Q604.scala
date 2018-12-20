package hmda.validation.rules.lar.quality

import akka.grpc.GrpcClientSettings
import com.typesafe.config.ConfigFactory
import hmda.grpc.services.{CensusServiceClient, ValidCountyRequest}
import hmda.model.filing.lar.LoanApplicationRegister
import hmda.validation.dsl.{
  ValidationFailure,
  ValidationResult,
  ValidationSuccess
}
import hmda.validation.rules.AsyncEditCheck
import hmda.validation.{AS, EC, MAT}

import scala.concurrent.Future

object Q604 extends AsyncEditCheck[LoanApplicationRegister] {
  override def name: String = "Q604"

  val config = ConfigFactory.load()

  val host = config.getString("hmda.census.grpc.host")
  val port = config.getInt("hmda.census.grpc.port")

  override def apply[as: AS, mat: MAT, ec: EC](
      lar: LoanApplicationRegister): Future[ValidationResult] = {

    val county = lar.geography.county
    val state = lar.geography.state

    if (county.toLowerCase != "na" && state.toLowerCase != "na") {
      countyIsValid(county).map {
        case true  => ValidationSuccess
        case false => ValidationFailure
      }
    } else {
      Future.successful(ValidationSuccess)
    }
  }

  def countyIsValid[as: AS, mat: MAT, ec: EC](
      county: String): Future[Boolean] = {
    val client = CensusServiceClient(
      GrpcClientSettings.connectToServiceAt(host, port).withTls(false)
    )
    for {
      response <- client
        .validateCounty(ValidCountyRequest(county))
        .map(response => response.isValid)
      _ <- client.close()
      closed <- client.closed()
    } yield (response, closed)._1
  }

}
