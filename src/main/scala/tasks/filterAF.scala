import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.util.TempFile
import tasks.upicklesupport._

import fileutils._

case class FilterAFInput(
    genomeFile: SharedFile,
    minAf: Double
)

case class FilterAFOutput(file: SharedFile) extends ResultWithSharedFiles(file)

object filterAFGenomeFile {

  val hliSampleSize = 7794
  val gExSampleSize = 123136
  val gGeSampleSize = 15496

  val task = AsyncTask[FilterAFInput, FilterAFOutput]("filterAF", 1) {

    case FilterAFInput(
        sf,
        minAf
        ) =>
      implicit ctx =>
        log.info(s"filter on ${sf.name}")

        sf.file.flatMap { localFile =>
          val tmpFile = openFileWriter { writer =>
            openSource(localFile)(s =>
              IOHelpers
                .readPreJoinedLocusFile2(s,
                                         hliSampleSize,
                                         gExSampleSize,
                                         gGeSampleSize)
                .filter(locus =>
                  (locus.alleleCountSyn + locus.alleleCountNonSyn + locus.alleleCountStopGain).toDouble / locus.sampleSize > minAf)
                .foreach { locus =>
                  writer.write(
                    locus.locus + "\t" + ((locus.alleleCountSyn + locus.alleleCountNonSyn + locus.alleleCountStopGain).toDouble / locus.sampleSize) + "\n")
              })
          }._1
          SharedFile(tmpFile, name = sf.name + ".affilter." + minAf + ".tsv")
            .map(x => FilterAFOutput(x))
        }

  }

}
