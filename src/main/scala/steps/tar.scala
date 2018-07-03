package sd.steps

import tasks._
import tasks.upicklesupport._
import akka.stream.scaladsl._
import fileutils._
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.utils.IOUtils

case class TarArchiveInput(files: Map[String, SharedFile], name: String)

object TarArchive {
  val archiveSharedFiles =
    AsyncTask[TarArchiveInput, SharedFile]("tarfiles-1", 1) {
      case TarArchiveInput(files, tarFileName) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          val file = openFileOutputStream { os =>
            val outputStream = new TarArchiveOutputStream(os)

            files.foreach {
              case (name, sf) =>
                val entry = new TarArchiveEntry(name)
                outputStream.putArchiveEntry(entry)
                val inputStream =
                  sf.source.runWith(StreamConverters.asInputStream())
                IOUtils.copy(inputStream, outputStream)
                outputStream.closeArchiveEntry
            }

            outputStream.finish()
          }._1

          SharedFile(file, name = tarFileName)
    }
}
