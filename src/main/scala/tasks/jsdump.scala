import java.io.{File, Closeable}
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.upicklesupport._
import tasks.collection._
import tasks.queue.NodeLocalCache
import tasks.util.TempFile
import fileutils._
import akka.util._
import akka.stream.scaladsl._

case class JsDump[T](sf: SharedFile) extends ResultWithSharedFiles(sf) {

  def createIterator[R](file: File)(
      implicit r: upickle.default.Reader[T],
      w: upickle.default.Writer[T]): (Iterator[T], Closeable) = {
    val source = createSource(file)
    val unpickler = implicitly[upickle.default.Reader[T]]
    val it =
      source.getLines.map(line => upickle.default.read[T](line)(unpickler))
    it -> new Closeable { def close = source.close }
  }

  def iterator[R](file: File)(f: Iterator[T] => R)(
      implicit r: upickle.default.Reader[T],
      w: upickle.default.Writer[T]): R =
    openSource(file) { source =>
      val unpickler = implicitly[upickle.default.Reader[T]]
      val it =
        source.getLines.map(line => upickle.default.read[T](line)(unpickler))
      f(it)
    }

  def source(implicit ec: ExecutionContext,
             ts: TaskSystemComponents,
             r: upickle.default.Reader[T],
             w: upickle.default.Writer[T]): Source[T, _] = {
    val unpickler = implicitly[upickle.default.Reader[T]]
    sf.source
      .via(Compression.gunzip())
      .via(akka.stream.scaladsl.Framing
        .delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue))
      .map { frame =>
        val line = frame.utf8String
        upickle.default.read[T](line)(unpickler)
      }
  }

}

object JsDump {

  def fromIterator[T: upickle.default.Reader: upickle.default.Writer](
      i: Iterator[T],
      name: String)(implicit ts: TaskSystemComponents): Future[JsDump[T]] = {
    implicit val ec = ts.executionContext
    val pickler = implicitly[upickle.default.Writer[T]]
    val tmp = openZippedFileWriter { writer =>
      i.foreach { elem =>
        writer.write(upickle.default.write(elem)(pickler) + "\n")
      }
    }._1
    SharedFile(tmp, name).map(x => JsDump(x))
  }
  def sink[T: upickle.default.Reader: upickle.default.Writer](name: String)(
      implicit ts: TaskSystemComponents): Sink[T, Future[JsDump[T]]] = {
    implicit val ec = ts.executionContext
    val tmp = TempFile.createTempFile("jdump" + name)
    val pickler = implicitly[upickle.default.Writer[T]]

    Flow[T]
      .map(x => ByteString(upickle.default.write(x)(pickler) + "\n"))
      .via(tasks.util.AkkaStreamComponents
        .strictBatchWeighted[ByteString](512 * 1024, _.size)(_ ++ _))
      .via(Compression.gzip)
      .toMat(FileIO.toPath(tmp.toPath))(Keep.right)
      .mapMaterializedValue { futureIODone =>
        futureIODone.flatMap {
          case x if x.status.isSuccess =>
            SharedFile(tmp, name).map(sf => JsDump[T](sf))
          case x => throw x.status.failed.get
        }
      }
  }
}
