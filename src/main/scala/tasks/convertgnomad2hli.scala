import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.collection._
import tasks.queue.NodeLocalCache
import tasks.upicklesupport._

import tasks.util.TempFile
import fileutils._
import SharedTypes._

import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import JoinVariationsCore.{GnomadLine, GnomadGenders, GnomadPop}

case class GnomadData(sf: SharedFile)

object ConvertGnomad2HLI {

  val toEColl =
    AsyncTask[JsDump[GnomadLine], EColl[GnomadLine]]("GenomeCoverageToEColl", 1) {
      js => implicit ctx =>
        implicit val mat = ctx.components.actorMaterializer
        log.info(s"Convert $js to ecoll.")
        EColl.partitionsFromSource(js.source, js.sf.name, 8)
    }

  def vcf2json(line: String): List[GnomadLine] =
    if (line.startsWith("#")) Nil
    else {
      val spl = line.split("\\t")
      val chr = spl(0)
      val position = spl(1).toInt
      val ref = spl(3)
      val alts = spl(4).split(",").toList
      val vcf_filter = spl(6)
      val anno = spl(7)
        .split(";")
        .map(_.split("="))
        .filter(_.size == 2)
        .map(x => x(0) -> x(1).split(","))
        .toMap

      def allele_counts(record: Map[String, Array[String]],
                        position: Int,
                        population_key: String) =
        scala.util.Try {

          val totalVariantAlleleCount =
            record("AC_" + population_key)(position).toInt
          val totalChromosomeCount = record("AN_" + population_key)(0).toInt

          GnomadPop(totalVariantAlleleCount, totalChromosomeCount)
        }.toOption

      alts.zipWithIndex.flatMap {
        case (alt, allele_position) =>
          val females = allele_counts(anno, allele_position, "Female")
          val males = allele_counts(anno, allele_position, "Male")

          if (females.isDefined && males.isDefined)
            GnomadLine("chr" + chr,
                       position,
                       ref,
                       alt,
                       vcf_filter,
                       GnomadGenders(females.get, males.get)) :: Nil
          else {
            println("line failed " + line)
            Nil
          }
      }

    }

  val task =
    AsyncTask[GnomadData, JsDump[GnomadLine]]("convertgnomad2hli", 1) {
      case GnomadData(sf) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          sf.source
            .via(Compression.gunzip())
            .via(Framing.delimiter(ByteString("\n"), Int.MaxValue, true))
            .map(_.utf8String)
            .mapConcat(line => vcf2json(line))
            .runWith(JsDump.sink(name = sf.name + ".js.gz"))

    }

}
