package sd

import stringsplit._
import fileutils.{openFileWriter, openSource}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.utils.IOUtils
import java.io._
import tasks.jsonitersupport._

case class Variant(locus: String,
                   variantClass: String,
                   features: Set[String],
                   alleleCount: Int)

case class LocusNsAndCodon(locus: String,
                           numNs: Int,
                           feature: String,
                           protein: String,
                           codon: Int)

case class Locus(locus: String,
                 numNs: Int,
                 features: Set[String],
                 protein: String,
                 codonNumber: Int,
                 alleleCountSyn: Int,
                 alleleCountNonSyn: Int,
                 alleleCountStopGain: Int,
                 sampleSize: Int) {
  def dropProteinData =
    LocusVariationCountAndNumNs(
      ChrPos(locus.split1('\t').take(3).mkString("\t")),
      numNs,
      alleleCountSyn,
      alleleCountNonSyn,
      sampleSize,
      3 - numNs)
}

case class LocusVariationCountAndNumNs(locus: ChrPos,
                                       numNs: Int,
                                       alleleCountSyn: Int,
                                       alleleCountNonSyn: Int,
                                       sampleSize: Int,
                                       numS: Int)

object LocusVariationCountAndNumNs {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[LocusVariationCountAndNumNs] =
    JsonCodecMaker.make[LocusVariationCountAndNumNs](CodecMakerConfig())

  implicit val serde = tasks.makeSerDe[LocusVariationCountAndNumNs]
}

sealed trait Strand
case object Forward extends Strand
case object Reverse extends Strand
case object MissingStrand extends Strand

case class GFFEntry(chr: String,
                    from: Int,
                    to: Int,
                    strand: Strand,
                    featureType: String,
                    attributes: Map[String, String],
                    name: String)
    extends intervaltree.Interval

object IOHelpers {

  def readUniProtIsoforms(source: scala.io.Source): Map[String, UniId] = {
    val lines = source.getLines
    val regexp = "^.*IsoId=([^;]*;).*$".r
    lines
      .filter(_.contains("Sequence=Displayed;"))
      .map { line =>
        line match {
          case regexp(isoid) => Some(isoid -> UniId(isoid.split1('-').head))
          case _             => None
        }

      }
      .filter(_.isDefined)
      .map(_.get)
      .toMap
  }

  def readUniProtFile(source: scala.io.Source): Iterator[UniProtEntry] = {
    val lines = source.getLines

    def records(it: Stream[String]): Stream[Seq[String]] = {
      if (it.isEmpty) Stream.empty
      else {
        val (head, tail) = it.span(x => !x.startsWith("//"))
        head.toList #:: records(tail.drop(1))
      }
    }

    val rec: Iterator[Seq[String]] = records(lines.toStream).toIterator

    rec.map { lines =>
      val accessions =
        lines
          .filter(_.startsWith("AC"))
          .flatMap(_.drop(2).split1(';'))
          .map(_.trim)
          .filter(_.size > 0)
          .map(s => UniId(s))

      val sqSize: Option[Int] = lines.find(_.startsWith("SQ")).map { line =>
        val firstEntry = line.split1Iter(';').next
        firstEntry.drop("SQ   SEQUENCE".size).dropRight(2).trim.toInt
      }

      val ensts: Seq[EnsT] =
        lines.filter(x => x.startsWith("DR") && x.contains("Ensembl;")).map {
          line =>
            EnsT(line.drop(5).split("; ")(1))
        }

      val pdbs =
        lines
          .filter(
            x =>
              x.startsWith("DR") && x.contains("PDB;") && (x
                .contains(" NMR;") || x.contains(" X-ray;")))
          .map { line =>
            // println(line)
            val entry = line.drop(5).split("; ")
            val pdbId = PdbId(entry(1).trim)
            val method = entry(2).trim.toLowerCase
            val res = if (method == "x-ray") {
              val x = entry(3).dropRight(1).trim
              if (x == "") None
              else Some(x.toDouble)
            } else Some(0.0)

            val rangeString = entry(4).reverse.dropWhile(_ == '.').reverse
            val chains: Seq[(PdbChain, List[(Int, Int)])] =
              if (rangeString == "-") Nil
              else {
                rangeString
                  .split(", ")
                  .flatMap { rangeString =>
                    val spl = rangeString.split1('=')
                    val chains = spl.head.split1('/')
                    val range = spl(1) match {
                      case "-" => Nil
                      case x =>
                        val spl = x.split1('-')
                        List(spl(0).toInt -> spl(1).toInt)
                    }
                    chains.map { chain =>
                      PdbChain(chain) -> range
                    }
                  }
                  .groupBy(_._1)
                  .toSeq
                  .map(x => x._1 -> x._2.map(_._2).flatten.toList)

              }

            (pdbId, PdbMethod(method), res, chains)
          }

      val sequence = lines
        .filter(_.startsWith("     "))
        .flatMap(_.filterNot(_ == ' '))
        .mkString("")

      val features: Seq[(String, UniNumber, UniNumber)] =
        lines.filter(_.startsWith("FT")).flatMap { ftLine =>
          val featureName = ftLine.substring(5, 13).trim
          val from =
            scala.util.Try(ftLine.substring(14, 20).trim.toInt).toOption
          val end =
            scala.util.Try(ftLine.substring(21, 27).trim.toInt).toOption
          if (UnneededProteinFeatures.contains(featureName)) Nil
          else if (from.isDefined && end.isDefined) {
            if (featureName == "DISULFID" || featureName == "CROSSLNK") {
              if (from.get != end.get)
                List(
                  (featureName, UniNumber(from.get - 1), UniNumber(from.get)),
                  (featureName, UniNumber(end.get - 1), UniNumber(end.get)))
              else
                List(
                  (featureName, UniNumber(from.get - 1), UniNumber(from.get)))
            } else
              List((featureName, UniNumber(from.get - 1), UniNumber(end.get)))
          } else Nil
        }

      val geneNames = lines
        .filter(_.startsWith("GN"))
        .flatMap { gnLine =>
          gnLine.trim.split1(';').map(_.drop(2).trim).flatMap { record =>
            if (record.startsWith("Name=")) {
              List(record.drop(5))
            } else if (record.startsWith("Synonyms=")) {
              record.drop("Synonyms=".size).split1(',').map(_.trim).toList
            } else Nil
          }
        }
        .map(GeneName(_))
        .toList

      UniProtEntry(
        accessions,
        sqSize,
        pdbs,
        UniSeq(sequence.map(c => if (c == 'U') 'C' else c)), // that is an aa code, not uracil
        ensts,
        features.toList,
        geneNames)

    }
  }

  val UnneededProteinFeatures = Set("UNSURE",
                                    "VARIANT",
                                    "CHAIN",
                                    "INIT_MET",
                                    "NON_TER",
                                    "NON_CONS",
                                    "CONFLICT")

  def readGencodeSwissProtMetadata(
      s: scala.io.Source): Iterator[(EnsT, UniId)] = s.getLines.map { line =>
    val spl = line.split1('\t')
    EnsT(spl(0).split1('.')(0)) -> UniId(spl(1))
  }

  def readGTF(s: scala.io.Source): Iterator[GFFEntry] =
    s.getLines
      .dropWhile(_.startsWith("#"))
      .map { line =>
        val spl = line.splitM('\t')
        val seqname = (new String(spl(0)))
        val feature = (new String(spl(2)))
        val start = spl(3).toInt
        val end = spl(4).toInt
        val strand = spl(6) match {
          case "+" => Forward
          case "-" => Reverse
          case "." => MissingStrand
        }
        val attributes = Map(
          spl(8)
            .splitM(';')
            .map(_.trim)
            .filter(_.size > 0)
            .map { t =>
              val spl = t.splitM(' ')
              (new String(spl(0))) -> (new String(
                spl.drop(1).mkString(" ").filterNot(_ == '"')))
            }
            .toList: _*)

        GFFEntry(seqname,
                 start - 1,
                 end,
                 strand,
                 feature,
                 attributes,
                 name = "")

      }
      .filterNot(_.strand == MissingStrand)

  def readGencodeGTF(
      s: scala.io.Source): Map[EnsT, (Seq[EnsE], TranscriptSupportLevel)] =
    readGTF(s)
      .filter(_.featureType == "exon")
      .map { gff =>
        (EnsT(gff.attributes("transcript_id").split1('.')(0)),
         EnsE(gff.from,
              gff.to,
              gff.chr,
              gff.strand,
              gff.attributes("exon_id").split1('.')(0),
              gff.attributes("exon_number").toInt),
         TranscriptSupportLevel(
           scala.util
             .Try(gff.attributes("transcript_support_level").toInt)
             .toOption
             .getOrElse(6)))
      }
      .toList
      .groupBy(_._1)
      .map(x => (x._1, (x._2.map(_._2).distinct, x._2.map(_._3).head)))

  def readGencodeProteinCodingTranscripts(
      s: scala.io.Source): Iterator[(EnsT, Transcript)] = {
    @specialized(Byte, Char)
    def tokenizeIterator[T](i: Iterator[T], sep: T): Iterator[Seq[T]] =
      new Iterator[Seq[T]] {
        def hasNext = i.hasNext

        def next = i.takeWhile(_ != sep).toSeq

      }
    def fastaIterator(source: scala.io.Source): Iterator[(String, String)] =
      tokenizeIterator(source, '>').drop(1).map { entry =>
        val spl = entry.mkString.splitMIter('\n')
        val header = spl.next
        val seq = spl.mkString("")
        (header, seq)
      }

    fastaIterator(s)
      .map {
        case (headerLine, sequence) =>
          val spl = headerLine.split1('|')
          val id = spl(0).split1Iter('.').next
          val cds = spl.find(_.startsWith("CDS:")).get.drop(4).split1('-')
          val start1 = cds(0).toInt
          val to1 = cds(1).toInt
          (id.startsWith("ENST"),
           EnsT(id),
           Transcript(start1 - 1, sequence.substring(start1 - 1, to1)))
      }
      .filter(_._1)
      .map(x => (x._2, x._3))

  }

  val three2One = Map(
    "ALA" -> 'A',
    "ARG" -> 'R',
    "ASN" -> 'N',
    "ASP" -> 'D',
    "ASX" -> 'B',
    "CYS" -> 'C',
    "GLU" -> 'E',
    "GLN" -> 'Q',
    "GLX" -> 'Z',
    "GLY" -> 'G',
    "HIS" -> 'H',
    "ILE" -> 'I',
    "LEU" -> 'L',
    "LYS" -> 'K',
    "MET" -> 'M',
    "MSE" -> 'M',
    "PHE" -> 'F',
    "PRO" -> 'P',
    "SER" -> 'S',
    "THR" -> 'T',
    "TRP" -> 'W',
    "TYR" -> 'Y',
    "VAL" -> 'V'
  )

  def readLigandability(source: scala.io.Source): Iterator[LigandabilityRow] = {
    import com.github.tototoshi.csv._
    val reader = com.github.tototoshi.csv.CSVReader.open(source)
    val lines = reader.iterator
    val header = lines.next
    lines.map { spl =>
      val uniId = UniId(spl(0))
      val uniNum = UniNumber(spl(2).split1('/').head.toInt - 1)
      val rest = ((header zip spl) toMap).filter(_._2.nonEmpty)

      LigandabilityRow(uniId, uniNum, rest)
    }
  }

// case class DepletionScoreCDFs(
//   nsPostMeanGlobalSynonymousRate : Seq[(Double,Double)],
//   nsPostMeanHeptamerSpecificIntergenicRate : Seq[(Double,Double)],
//   nsPostMeanHeptamerIndependentIntergenicRate : Seq[(Double,Double)],
//   nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate : Seq[(Double,Double)],
//   nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate : Seq[(Double,Double)]
// )
  def writeCDFs(d: DepletionScoreCDFs): File = {
    (openFileWriter { writer =>
      writer.write(
        d.nsPostMeanGlobalSynonymousRate
          .map(_.productIterator.mkString(","))
          .mkString(",") + "\n")
      writer.write(
        d.nsPostMeanHeptamerSpecificIntergenicRate
          .map(_.productIterator.mkString(","))
          .mkString(",") + "\n")
      writer.write(
        d.nsPostMeanHeptamerIndependentIntergenicRate
          .map(_.productIterator.mkString(","))
          .mkString(",") + "\n")
      writer.write(
        d.nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate
          .map(_.productIterator.mkString(","))
          .mkString(",") + "\n")
      writer.write(
        d.nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate
          .map(_.productIterator.mkString(","))
          .mkString(",") + "\n")
    })._1
  }

  def readCDFs(f: File): DepletionScoreCDFs = openSource(f) { s =>
    val lines = s.getLines.map { line =>
      line
        .split1(',')
        .map(_.toDouble)
        .grouped(2)
        .map(s => s(0) -> s(1))
        .toVector
    }.toVector
    DepletionScoreCDFs(lines(0), lines(1), lines(2), lines(3), lines(4))
  }
  def readSwissmodelMetadata(tar: File, isoforms: Map[String, UniId]) = {
    val tarInput =
      new TarArchiveInputStream(
        new java.util.zip.GZIPInputStream(new FileInputStream(tar)))
    val index = Iterator
      .continually(tarInput.getNextTarEntry)
      .take(10)
      .find(x => x != null && x.getName == "SWISS-MODEL_Repository/INDEX")
      .get
    val data = Array.ofDim[Byte](index.getSize.toInt)
    IOUtils.readFully(tarInput, data)
    tarInput.close
    val indexString = new String(data, "UTF-8")
    scala.io.Source
      .fromString(indexString)
      .getLines
      .dropWhile(_.startsWith("#"))
      .drop(1)
      .map { _.split1('\t') }
      .filter(spl => spl(5).trim == "SWISSMODEL")
      .filter(spl => spl.size >= 13)
      .flatMap { spl =>
        val rawUniID = spl(1)
        val uniID =
          if (!rawUniID.contains("-")) Some(rawUniID)
          else isoforms.get(rawUniID).map(_.s)
        val hash = spl(4)
        val from = spl(6).toInt
        val to = spl(7).toInt
        val template = spl(8)
        val qmean = spl(9).toDouble
        val url = spl(12)

        uniID.map { uniID =>
          val filename = uniID + "_" + from + "_" + to + "_" + template + "_" + hash
          (filename, qmean, url, uniID, from, to)
        }.iterator
      }
      .filter { case (_, qmean, _, _, _, _) => qmean >= -4.0 }
      .toList
  }

}
