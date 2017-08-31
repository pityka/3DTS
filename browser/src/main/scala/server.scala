import org.scalajs.dom
import org.scalajs.dom.ext.{Ajax, AjaxException}
import scalatags.JsDom._
import org.scalajs.dom.raw._
import all._
import tags2.section
import rx._
import rx.async._
import scala.util._
import scala.scalajs.js.annotation.JSExport
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.concurrent.Future
import scala.scalajs._
import scala.scalajs.js._
import framework.Framework._
import upickle.default.Reader
import SharedTypes._

object Server {
  def query(qt: String): Future[ServerReturn] =
    Ajax.get("/query?q=" + qt).map(_.responseText).map { responseText =>
      println("received data" + responseText.size)
      val r: ServerReturn =
        upickle.default.read[ServerReturn](responseText)
      println(r._1.headOption)
      println(r._2.headOption)
      r
    }

}
