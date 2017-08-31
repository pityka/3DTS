import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.util.TempFile
import fileutils._

case class GencodeUniprotInput(
    gencodeGtf: SharedFile,
    ensemblXrefUniprot: SharedFile,
    transcriptFasta: SharedFile,
    uniprotKb: SharedFile
)

object gencodeUniprot {

  val task = AsyncTask[GencodeUniprotInput, JsDump[Ensembl2Uniprot.MapResult]](
    "gencodeuniprot-2",
    5) {
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

            log.info("gtf read")

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

            log.info("uniprot kb read")

            openSource(fastaL) { fastaSource =>
              val s: Iterator[Ensembl2Uniprot.MapResult] =
                Ensembl2Uniprot.mapTranscripts(
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
