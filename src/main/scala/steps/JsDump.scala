// package sd.steps

// import java.io.{File, Closeable}
// import scala.concurrent._
// import tasks._
// import tasks.util.TempFile
// import fileutils._
// import akka.util._
// import akka.stream.scaladsl._
// import com.github.plokhotnyuk.jsoniter_scala.macros._
// import com.github.plokhotnyuk.jsoniter_scala.core._

// case class JsDump[T](sf: SharedFile) {

//   def createIterator[R](file: File)(
//       implicit r: JsonValueCodec[T]): (Iterator[T], Closeable) = {
//     val source = createSource(file)
//     val it =
//       source.getLines.map(line => readFromString[T](line))
//     it -> new Closeable { def close = source.close }
//   }

//   def iterator[R](file: File)(f: Iterator[T] => R)(
//       implicit r: JsonValueCodec[T]): R =
//     openSource(file) { source =>
//       val it =
//         source.getLines.map(line => readFromString[T](line))
//       f(it)
//     }

//   def source(implicit
//              ts: TaskSystemComponents,
//              r: JsonValueCodec[T]): Source[T, akka.NotUsed] = {
//     sf.source
//       .via(Compression.gunzip())
//       .via(akka.stream.scaladsl.Framing
//         .delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue))
//       .map { frame =>
//         val line = frame.utf8String
//         readFromString[T](line)
//       }
//   }

// }

// object JsDump {

//   implicit def codec[T: JsonValueCodec]: JsonValueCodec[JsDump[T]] =
//     JsonCodecMaker.make[JsDump[T]](CodecMakerConfig())

//   def fromIterator[T: JsonValueCodec](i: Iterator[T], name: String)(
//       implicit ts: TaskSystemComponents): Future[JsDump[T]] = {
//     implicit val ec = ts.executionContext
//     val tmp = openZippedFileWriter { writer =>
//       i.foreach { elem =>
//         writer.write(writeToString(elem) + "\n")
//       }
//     }._1
//     SharedFile(tmp, name).map(x => JsDump(x))
//   }
//   def sink[T: JsonValueCodec](name: String)(
//       implicit ts: TaskSystemComponents): Sink[T, Future[JsDump[T]]] = {
//     implicit val ec = ts.executionContext
//     val tmp = TempFile.createTempFile("jdump" + name)

//     Flow[T]
//       .map(x => ByteString(writeToString(x) + "\n"))
//       .via(tasks.util.AkkaStreamComponents
//         .strictBatchWeighted[ByteString](512 * 1024, _.size)(_ ++ _))
//       .via(Compression.gzip)
//       .toMat(FileIO.toPath(tmp.toPath))(Keep.right)
//       .mapMaterializedValue { futureIODone =>
//         futureIODone.flatMap {
//           case x if x.status.isSuccess =>
//             SharedFile(tmp, name).map(sf => JsDump[T](sf))
//           case x => throw x.status.failed.get
//         }
//       }
//   }
// }
