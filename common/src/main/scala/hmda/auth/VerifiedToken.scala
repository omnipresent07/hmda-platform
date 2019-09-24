package hmda.auth

case class VerifiedToken(token: String, id: String, name: String, username: String, email: String, roles: Seq[String], lei: String)

object VerifiedToken {
  def apply(): VerifiedToken =
    VerifiedToken("empty-token", "dev", "token", "dev", "bhaarat.s@gmail.com", Seq.empty, "lei")
}
