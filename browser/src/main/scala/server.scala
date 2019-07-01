package sd.ui

import org.scalajs.dom.ext.Ajax
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.concurrent.Future
import sd.shared._

object Server {
  type ServerReturn = (Seq[PdbId], Seq[DepletionScoresByResidue])
  def getCdfs: Future[DepletionScoreCDFs] =
    Ajax.get("/cdfs").map(_.responseText).map { r =>
      println(r.take(100))
      upickle.default.read[DepletionScoreCDFs](r)
    }
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
