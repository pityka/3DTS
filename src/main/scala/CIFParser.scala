package sd

object CIFParser extends App {
  val assembly = args(0).toBoolean
  val cifContents = CIFContents
    .parseCIF(scala.io.Source.stdin.getLines.toList, !assembly)
  cifContents match {
    case scala.util.Success(cif) =>
      println(upickle.default.write(cif))
    case scala.util.Failure(e) =>
      println(e)
      System.exit(1)
  }
}
