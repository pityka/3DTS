package sd.steps

import sd._
import tasks._
import tasks.jsonitersupport._
import fileutils._
import tasks.ecoll._

case class GencodeUniprotInput(
    gencodeGtf: SharedFile,
    ensemblXrefUniprot: SharedFile,
    transcriptFasta: SharedFile,
    uniprotKb: SharedFile
)
object GencodeUniprotInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[GencodeUniprotInput] =
    JsonCodecMaker.make[GencodeUniprotInput](sd.JsonIterConfig.config)
}

object JoinGencodeToUniprot {

  val countMappedUniprot =
    AsyncTask[EColl[sd.JoinGencodeToUniprot.MapResult], Int](
      "joingencodeuniprot-count-mapped-uniprot",
      2) {
      case js =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          val set = scala.collection.mutable.Set[UniId]()

          js.source(resourceAllocated.cpu)
            .collect {
              case sd.JoinGencodeToUniprot.Success(mapped) => mapped
            }
            .mapConcat(_.map(_.uniId).toList)
            .runForeach { uniid =>
              set.add(uniid)
            }
            .map { _ =>
              set.size
            }

    }

  val countMappedEnst =
    AsyncTask[EColl[sd.JoinGencodeToUniprot.MapResult], Int](
      "joingencodeuniprot-count-mapped-enst",
      2) {
      case js =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          val set = scala.collection.mutable.Set[EnsT]()

          js.source(resourceAllocated.cpu)
            .collect {
              case sd.JoinGencodeToUniprot.Success(mapped) => mapped
            }
            .mapConcat(_.map(_.ensT).toList)
            .runForeach { enst =>
              set.add(enst)
            }
            .map { _ =>
              set.size
            }

    }

  val task =
    AsyncTask[GencodeUniprotInput, EColl[sd.JoinGencodeToUniprot.MapResult]](
      "gencodeuniprot-3",
      1) {
      case GencodeUniprotInput(
          gencodeGtf,
          xref,
          fasta,
          unikb
          ) =>
        implicit ctx =>
          log.info("gencode uniprot start")
          for {
            gencodeGtfL <- gencodeGtf.file
            fastaL <- fasta.file
            ensemblXrefUniprotL <- xref.file
            uniprotKbL <- unikb.file
            ret <- {

              val gencode =
                openSource(gencodeGtfL)(s => IOHelpers.readGencodeGTF(s))

              log.info("gtf read: " + gencode.size)

              val ensembleXRefUniProt = openSource(ensemblXrefUniprotL)(s =>
                IOHelpers.readGencodeSwissProtMetadata(s).toList.toMap)

              log.info("xref read")

              val uniprotKb =
                openSource(uniprotKbL)(
                  s =>
                    IOHelpers
                      .readUniProtFile(s)
                      .flatMap(x => x.accessions.map(y => y -> x))
                      .toMap)

              log.info("uniprot kb read. size: " + uniprotKb.size)

              val fastaSource = createSource(fastaL)
              val s: Iterator[sd.JoinGencodeToUniprot.MapResult] =
                sd.JoinGencodeToUniprot.mapTranscripts(
                  gencode,
                  ensembleXRefUniProt,
                  IOHelpers.readGencodeProteinCodingTranscripts(fastaSource),
                  uniprotKb)
              EColl
                .fromIterator(s, gencodeGtf.name + ".genome.json.gz")
                .andThen {
                  case _ => fastaSource.close
                }

            }
          } yield ret

    }
}
