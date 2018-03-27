import fileutils._
import stringsplit._
// import org.saddle._
import java.io._

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

  def readLigandContext(f: String)
    : List[(String, Seq[(PdbId, PdbChain, PdbResidueNumberUnresolved)])] =
    openSource(f)(
      _.getLines
        .drop(1)
        .map { line =>
          val spl = line.split1('\t')
          (spl(0),
           PdbId(spl(1)),
           PdbChain(spl(2)),
           PdbResidueNumberUnresolved(spl(3)))
        }
        .toList
        .groupBy(_._1)
        .toList
        .map(x => x._1 -> x._2.map(y => (y._2, y._3, y._4))))

  def readSNVLociFromVcf(f: String, grep: Option[String] = None) =
    openSource(f)(
      _.getLines
        .filterNot(_.startsWith("#"))
        .filter(l => grep.map(g => l.contains(g)).getOrElse(true))
        .map { line =>
          val spl = line.split1('\t')
          List(spl(0), spl(1), spl(1).toInt + 1).mkString("\t") -> spl(4).size
        }
        .filter(_._2 == 1)
        .map(_._1)
        .toSet)

  def readPreJoinedLocusFile2(source: scala.io.Source,
                              hliSampleSize: Int,
                              gnomadExomeSampleSize: Int,
                              gnomadGenomeSampleSize: Int) =
    source.getLines
      .map { line =>
        val spl = line.split1('\t')
        if (spl.size >= 44) {
          val cp = spl.take(4).mkString("\t")
          val protein = spl(4)
          val codonNumber = {
            val s = spl(8)
            if (s == "*") -1
            else s.toInt
          }
          val features = spl(9).split1(',').toSet.filterNot(_ == "NO_FEATURE")

          def clz(s: String) = s.split1(',')

          def ac(s: String, mult: Int, hc: Boolean) =
            if (!hc)
              1 to mult map (_ => 0)
            else
              s match {
                case "NaN" => 1 to mult map (_ => 0)
                case x     => x.split1(',').map(_.toInt)
              }

          def an(s: String, mult: Int, sampleSize: Int, hc: Boolean) =
            if (!hc)
              1 to mult map (_ => 0)
            else
              s match {
                case "NaN" => 1 to mult map (_ => sampleSize)
                case x     => x.split1(',').map(_.toInt)
              }

          // (s,ns,sg,sample)
          def zip(clz: Seq[String],
                  ac: Seq[Int],
                  an: Seq[Int]): (Int, Int, Int, Int) = {

            val sampleSize = an.head / 2
            val acs = clz zip ac map {
              case (clz, ac) =>
                clz match {
                  case "non-synonymous"                           => (0, ac, 0)
                  case "synonymous"                               => (ac, 0, 0)
                  case "stop-gained" | "start-lost" | "stop-lost" => (0, 0, ac)
                  case _                                          => (0, 0, 0)
                }
            } reduce ((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
            (acs._1, acs._2, acs._3, sampleSize)
          }

          val hc = spl(42).split1(',').map(_.toInt)

          val hliHc = hc(0) == 1
          val gnomadExomeHc = hc(2) == 1
          val gnomadGenomeHc = hc(1) == 1

          val hliMult = spl(10).split1(',').size
          val gnomadExomeMult = spl(26).split1(',').size
          val gnomadGenomeMult = spl(34).split1(',').size

          val hliClass = clz(spl(16))
          val gnomadExomeClass = clz(spl(32))
          val gnomadGenomeClass = clz(spl(40))

          val hliAc = ac(spl(13), hliMult, hliHc)
          val gnomadExomeAc = ac(spl(29), gnomadExomeMult, gnomadExomeHc)
          val gnomadGenomeAc = ac(spl(37), gnomadGenomeMult, gnomadGenomeHc)

          val hliAn = an(spl(14), hliMult, hliSampleSize, hliHc)
          val gnomadExomeAn =
            an(spl(30), gnomadExomeMult, gnomadExomeSampleSize, gnomadExomeHc)
          val gnomadGenomeAn =
            an(spl(38),
               gnomadGenomeMult,
               gnomadGenomeSampleSize,
               gnomadGenomeHc)

          if (hliAn.distinct.size > 1 || gnomadExomeAn.distinct.size > 1 || gnomadGenomeAn.distinct.size > 1)
            None
          else {

            val numNs = spl(43).toInt

            val hliSum = zip(hliClass, hliAc, hliAn)
            val gnomadExomeSum =
              zip(gnomadExomeClass, gnomadExomeAc, gnomadExomeAn)
            val gnomadGenomeSum =
              zip(gnomadGenomeClass, gnomadGenomeAc, gnomadGenomeAn)

            val sum =
              List(hliSum, gnomadExomeSum, gnomadGenomeSum).reduce((x, y) =>
                (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))

            Some(Locus(
              locus = cp,
              numNs = numNs,
              features = features,
              protein = protein,
              codonNumber = codonNumber,
              alleleCountSyn = sum._1,
              alleleCountNonSyn = sum._2,
              alleleCountStopGain = sum._3,
              sampleSize = sum._4
            ))
          }
        } else None
      }
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.sampleSize > 0)

  def readPreJoinedLocusFile(f: String, sampleSize: Int) =
    openSource(f)(
      _.getLines
        .map { line =>
          val spl = line.split1('\t')
          val cp = spl.take(4).mkString("\t")
          val protein = spl(4)
          val codonNumber = {
            val s = spl(8)
            if (s == "*") -1
            else s.toInt
          }
          val hc = spl(9).toInt == 1
          val features = spl(10).split1(',').toSet.filterNot(_ == "NO_FEATURE")
          val variantClass = spl(11)
            .split1(',')
            .filterNot(_ == "NO_non-synonymous_OR_synonymous_OR_stop-gained")
            .sortBy(_ match {
              case "stop-gained"    => 0
              case "synonymous"     => 2
              case "non-synonymous" => 1
            })
            .headOption

          val ac = spl(12) match {
            case "NO_AC" => 0
            case x =>
              x.split1(',')
                .flatMap(_.split1(';'))
                .filter(_.size > 0)
                .map(_.toInt)
                .sum
          }

          // number of good calls, not used at the moment
          val an = spl(13) match {
            case "NO_AN" => 0
            case x =>
              x.split1(',')
                .flatMap(_.split1(';'))
                .filter(_.size > 0)
                .map(_.toInt)
                .sum
          }

          val numNs = spl(14).toInt

          hc -> Locus(
            cp,
            numNs,
            features,
            protein,
            codonNumber,
            if (variantClass == Set("synonymous")) ac else 0,
            if (variantClass.contains("non-synonymous")) ac else 0,
            if (variantClass.contains("stop-gain")) ac else 0,
            sampleSize
          )
        }
        .filter(_._1)
        .map(_._2)
        .toList)

  def joinVarianstAndNsLists(variants: List[Variant],
                             ns: List[LocusNsAndCodon],
                             sampleSize: Int): List[Locus] = {
    val loci = variants.map(x => x.locus -> x).toMap
    ns.map { x =>
      val variantClass: Set[String] =
        loci.get(x.locus).map(_.variantClass).toSet
      val ac = loci.get(x.locus).map(_.alleleCount).getOrElse(0)

      Locus(
        x.locus,
        x.numNs,
        loci.get(x.locus).map(_.features).getOrElse(Set()) + x.feature,
        x.protein,
        x.codon,
        if (variantClass == Set("synonymous")) ac else 0,
        if (variantClass.contains("non-synonymous")) ac else 0,
        if (variantClass.contains("stop-gain")) ac else 0,
        sampleSize
      )
    }.toList
  }

  def readVariants(
      f: String): Seq[(String, Set[String], Set[String], Int, String)] =
    openSource(f)(
      _.getLines
        .map { line =>
          val spl = line.split1('\t')
          val cpra = spl.take(4).mkString("\t")
          val variantClass = spl(6)
          val population = spl(7)
          val ac = spl(8).toInt
          val feature = spl(9)
          (cpra, variantClass, population, feature, ac)
        }
        .toList
        .groupBy(_._1)
        .toSeq
        .map { x =>
          (x._2.head._2,
           x._2.map(_._3).toSet,
           x._2.map(_._4).toSet,
           x._2.head._5,
           x._2.head._1)
        })

  def readFeatures(f: String) =
    openSource(f)(
      _.getLines
        .map { line =>
          line.split1('\t')
        }
        .filter(_.size == 5)
        .map { spl =>
          (spl.take(3) :+ spl(4)).mkString("\t") -> spl(3)
        }
        .toList)

  def readLoci(f: String) =
    openSource(f)(
      _.getLines
        .flatMap { line1 =>
          val spl1 = line1.split1('\t')
          line1.split1(';').map {
            line =>
              val spl = line.split1('\t')
              if (spl.size == 4) {

                val cp = spl1.take(4).mkString("\t")
                val numNS = spl1(7).toInt
                val codon = spl1(6).toInt
                val feature = if (spl.size < 4) "" else spl(3)
                (cp, numNS, feature, codon)
              } else {
                val cp = spl.take(4).mkString("\t")
                val numNS = spl(7).toInt
                val codon = spl(6).toInt
                val feature = if (spl.size < 9) "" else spl(8)
                (cp, numNS, feature, codon)
              }
          }
        }
        .toList
        .foldLeft(List[(String, Int, String, Int, Int)]()) { (acc, elem) =>
          if (acc.isEmpty) (elem._1, elem._2, elem._3, elem._4, 0) :: acc
          else if (acc.head._4 <= elem._4)
            (elem._1, elem._2, elem._3, elem._4, acc.head._5) :: acc
          else (elem._1, elem._2, elem._3, elem._4, acc.head._5 + 1) :: acc
        }).map(x => LocusNsAndCodon(x._1, x._2, x._3, x._4.toString, x._5))

  def read5AngstromFile(f: String) =
    openSource(f)(_.getLines.flatMap { line =>
      val spl = line.split1('\t')
      val cpra = spl.take(4).mkString("\t")
      val protein = spl(4)
      val features = spl(5).split1(',')
      features.map { f =>
        (cpra, protein + "_" + f)
      }
    }.toList)

  def readHighConfidenceRegion(f: String) =
    openSource(f)(
      _.getLines
        .map { line =>
          val spl = line.split1('\t')
          val cpra = spl.take(4).mkString("\t")
          val hc = spl(5) == "1"
          (cpra, hc)
        }
        .filter(_._2)
        .map(_._1)
        .toSet)

  def readGnomadFile(f: String) =
    openSource(f)(_.getLines.map { line =>
      val spl = line.split1('\t')
      val cpra = spl.take(4).mkString("\t")
      val cl = spl(5)
      (cpra, cl)
    }.toList)

  def readLociQ8(f: String) =
    openSource(f)(
      _.getLines.toList
        .dropRight(1)
        .map { line =>
          val spl = line.split1('\t')
          val cpra = spl.take(4).mkString("\t")
          val numNS = spl.last.toInt
          val codon = spl(8).toInt
          (cpra, numNS, "", codon, 0)
        }
        .toList)

  def readVariantsQ8(f: String) =
    openSource(f)(
      _.getLines.toList
        .dropRight(1)
        .map { line =>
          val spl = line.split1('\t')
          val cpra = spl.take(4).mkString("\t")
          val variantClass =
            if (line.contains("non-synonymous")) "non-synonymous"
            else if (line.contains("synonymous")) "synonymous"
            else ""
          (cpra, variantClass)
        }
        .filter(_._2 != ""))

  def extractZipArchive(f: File, destDir: File): Unit = {
    import java.util.zip._

    val BUFFER_SIZE = 4096;

    def extractFile(zipIn: ZipInputStream, filePath: String): Unit = {

      new File(filePath).getParentFile.mkdirs

      val bos = new BufferedOutputStream(new FileOutputStream(filePath));
      com.google.common.io.ByteStreams.copy(zipIn, bos)
      bos.close
    }

    if (!destDir.exists()) {
      destDir.mkdir();
    }
    val zipIn = new ZipInputStream(new FileInputStream(f));
    var entry = zipIn.getNextEntry();
    // iterates over entries in the zip file
    while (entry != null) {
      val filePath = destDir.getAbsolutePath + File.separator + entry
        .getName();
      if (!entry.isDirectory()) {
        // if the entry is a file, extracts it
        extractFile(zipIn, filePath);
      } else {
        // if the entry is a directory, make the directory
        val dir = new File(filePath);
        dir.mkdir();
      }
      zipIn.closeEntry();
      entry = zipIn.getNextEntry();
    }
    zipIn.close();

  }

  def readPliCsv(s: scala.io.Source) = s.getLines.drop(1).map { line =>
    val spl = line.split1(',')
    GeneSymbol(spl(1)) -> spl(19).toDouble
  }

  def readGeneSymbolsFromUniProt(
      source: scala.io.Source): Iterator[(UniId, GeneSymbol)] = {
    val lines = source.getLines

    def records(it: Stream[String]): Stream[Seq[String]] = {
      if (it.isEmpty) Stream.empty
      else {
        val (head, tail) = it.span(x => !x.startsWith("//"))
        head.toList #:: records(tail.drop(1))
      }
    }

    val rec: Iterator[Seq[String]] = records(lines.toStream).toIterator
    rec.flatMap { record =>
      val accessions =
        record
          .filter(_.startsWith("AC"))
          .flatMap(_.drop(2).split1(';'))
          .map(_.trim)
          .filter(_.size > 0)
          .map(s => UniId(s))

      val geneSymbol = record
        .filter(_.startsWith("GN"))
        .flatMap { gnline =>
          gnline
            .drop(5)
            .split(";\\s*")
            .filter(_.contains("="))
            .map(_.split("=")(1).trim)
            .map(_.split("[\\s,]+")(0))
        }
        .map(s => GeneSymbol(s))

      geneSymbol.toList.flatMap { g =>
        accessions.map { ac =>
          ac -> g
        }
      }

    }
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

  def chunkContextFiles(s: scala.io.Source): Seq[(String, File)] = {
    val files = scala.collection.mutable.ArrayBuffer[(String, File)]()

    var iter = s.getLines.take(1000)
    while (iter.hasNext) {
      val first = iter.next
      val pdbId = first.split('\t')(1)
      val (head, tail) = iter.span { line =>
        val spl = line.split1Iter('\t')
        spl.next
        spl.next == pdbId
      }
      files.append(pdbId -> writeToTempFile(head.mkString("\n")))
      iter = tail
    }

    files
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

  def readUniProtGenomicMap(
      s: scala.io.Source): Iterator[(ChrPos, UniId, UniNumber)] =
    s.getLines.map { line =>
      val it = line.split1Iter('\t').take(9).toArray
      val c = ChrPos(it.take(3).mkString("\t"))
      val uni = UniId(it(4))
      val uninum = UniNumber(it(8).toInt)
      (c, uni, uninum)

    }

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

}
