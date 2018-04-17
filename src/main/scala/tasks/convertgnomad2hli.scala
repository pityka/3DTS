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
import stringsplit._

case class GnomadData(sf: SharedFile)

object ConvertGnomad2HLI {

  val toEColl =
    AsyncTask[JsDump[GnomadLine], EColl[GnomadLine]]("GenomeCoverageToEColl", 1) {
      js => implicit ctx =>
        implicit val mat = ctx.components.actorMaterializer
        log.info(s"Convert $js to ecoll.")
        EColl.fromSource(js.source, js.sf.name, 1024 * 1024 * 10)
    }

  def vcf2json(line: String): List[GnomadLine] =
    if (line.startsWith("#")) Nil
    else {
      val neededAnnotations =
        List("AC_Female", "AN_Female", "AC_Male", "AN_Male")
      val spl = line.split1('\t')
      val chr = spl(0)
      val position = spl(1).toInt
      val ref = spl(3)
      val alts = spl(4).split(",").toList
      val vcf_filter = spl(6)
      val anno = spl(7)
        .split1(';')
        .filter(_.startsWith("A"))
        .map(_.split1('='))
        .filter(_.size == 2)
        .map(x => x(0) -> x(1).split(","))
        .filter(x => neededAnnotations.contains(x._1))
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
            println(
              "Line failed: " + line + "\n" + neededAnnotations + "\n" + anno + "\n" + females + "\n" + males)
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

  val gnomadToEColl =
    AsyncTask[List[GnomadData], EColl[GnomadLine]]("convertgnomad2hli-ecoll", 1) {
      case files =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          val convertpar = math.max(1, resourceAllocated.cpu / 2)
          val ecollPar = math.max(1, resourceAllocated.cpu - convertpar)

          log.info(
            "Start converting " + files + " " + convertpar + " " + ecollPar)

          val converterFlow: Flow[String, GnomadLine, _] =
            tasks.util.AkkaStreamComponents.parallelize(convertpar, 1000)(
              vcf2json)

          val source = Source(files.map(_.sf)).flatMapConcat { sf =>
            sf.source
              .via(Compression.gunzip())
              .via(tasks.util.AkkaStreamComponents
                .delimiter('\n', maximumFrameLength = Int.MaxValue))
              .map(_.utf8String)
              .via(converterFlow)
          }

          EColl.fromSource(source,
                           files.head.sf.name,
                           1024 * 1024 * 50,
                           parallelism = ecollPar)

    }

}
