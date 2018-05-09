package sd.ui

import sd._
import org.scalajs.dom.ext.Ajax
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.concurrent.Future
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

  def post(data: String, topic: String) = {
    Ajax.post("/feedback/" + topic, data = data)
  }

}
