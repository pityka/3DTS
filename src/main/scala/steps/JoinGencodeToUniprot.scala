package sd.steps

import sd._
import tasks._
import tasks.upicklesupport._
import fileutils._

case class GencodeUniprotInput(
    gencodeGtf: SharedFile,
    ensemblXrefUniprot: SharedFile,
    transcriptFasta: SharedFile,
    uniprotKb: SharedFile
)

object JoinGencodeToUniprot {

  val countMappedUniprot =
    AsyncTask[JsDump[sd.JoinGencodeToUniprot.MapResult], Int](
      "joingencodeuniprot-count-mapped-uniprot",
      2) {
      case js =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          val set = scala.collection.mutable.Set[UniId]()

          js.source
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
    AsyncTask[JsDump[sd.JoinGencodeToUniprot.MapResult], Int](
      "joingencodeuniprot-count-mapped-enst",
      2) {
      case js =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          val set = scala.collection.mutable.Set[EnsT]()

          js.source
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
    AsyncTask[GencodeUniprotInput, JsDump[sd.JoinGencodeToUniprot.MapResult]](
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

              openSource(fastaL) { fastaSource =>
                val s: Iterator[sd.JoinGencodeToUniprot.MapResult] =
                  sd.JoinGencodeToUniprot.mapTranscripts(
                    gencode,
                    ensembleXRefUniProt,
                    IOHelpers.readGencodeProteinCodingTranscripts(fastaSource),
                    uniprotKb)
                JsDump.fromIterator(s, gencodeGtf.name + ".genome.json.gz")

              }

            }
          } yield ret

    }
}
