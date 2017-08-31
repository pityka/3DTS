package mypackage
import java.lang.management.ManagementFactory
import java.lang.management.RuntimeMXBean
import scala.collection.JavaConverters._
import scala.sys.process._

object MyApp extends App {
  println("App args: " + args.mkString("\"", "\",\"", "\""))
  println(
    "JVM args: " + ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .mkString("\"", "\",\"", "\""))

  println("Packaging..")
  selfpackage.write(new java.io.File("me"))
  "chmod u+x me" !
}
