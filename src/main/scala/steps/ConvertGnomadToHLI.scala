package sd.steps

import sd._
import tasks._
import tasks.ecoll._
import tasks.jsonitersupport._
import akka.stream.scaladsl._
import akka.util.ByteString
import JoinVariationsCore.{GnomadLine, GnomadGenders, GnomadPop}
import stringsplit._

case class GnomadData(sf: SharedFile)

case class GnomadDataList(data: Seq[GnomadData])

object ConvertGnomadToHLI {

  val toEColl =
    AsyncTask[JsDump[GnomadLine], EColl[GnomadLine]]("GenomeCoverageToEColl", 1) {
      js => implicit ctx =>
        log.info(s"Convert $js to ecoll.")
        EColl.fromSource(js.source, js.sf.name, 1024 * 1024 * 10)
    }

  def vcf2json(line: String): List[GnomadLine] =
    if (line.startsWith("#")) Nil
    else {
      val neededAnnotations =
        List("AC_Female", "AN_Female", "AC_Male", "AN_Male")
      val spl = line.split1Array('\t')
      val chr = spl(0)
      val position = spl(1).toInt
      val ref = spl(3)
      val alts = spl(4).split1Array(',').toList
      val vcf_filter = spl(6)
      val anno = spl(7)
        .split1(';')
        .filter(_.startsWith("A"))
        .map(_.split1('='))
        .filter(_.size == 2)
        .map(x => x(0) -> x(1).split1Array(','))
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
    AsyncTask[GnomadDataList, EColl[GnomadLine]]("convertgnomad2hli-ecoll", 1) {
      case GnomadDataList(files) =>
        implicit ctx =>
          val convertpar = math.max(1, resourceAllocated.cpu / 2)
          val ecollPar = math.max(1, resourceAllocated.cpu - convertpar)

          log.info(
            "Start converting " + files + " " + convertpar + " " + ecollPar)

          val converterFlow: Flow[String, GnomadLine, akka.NotUsed] =
            tasks.util.AkkaStreamComponents.parallelize(convertpar, 1000)(
              vcf2json)

          val source = Source(files.toList.map(_.sf)).flatMapConcat { sf =>
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

object GnomadData {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[GnomadData] =
    JsonCodecMaker.make[GnomadData](CodecMakerConfig())
}
object GnomadDataList {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[GnomadDataList] =
    JsonCodecMaker.make[GnomadDataList](CodecMakerConfig())
}
