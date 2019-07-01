package sd

import com.typesafe.scalalogging.StrictLogging

case class GeneName(value: String)

object JoinUniprotWithPdb extends StrictLogging {

  val blosum = {
    val s = scala.io.Source
      .fromInputStream(getClass.getResourceAsStream("/BLOSUM62.txt"))
    val lines = s.getLines.toList.map(_.split("\\s+"))
    s.close
    lines.tail.zipWithIndex.flatMap {
      case (line, _) =>
        lines.head.drop(1).zipWithIndex.map {
          case (c1, i1) =>
            val c2 = line.head
            val score = line.tail(i1)
            (c1.charAt(0), c2.charAt(0)) -> score.toInt
        }
    }.toMap
  }

  /** Outer joins the two sequences
    *
    * @return (list,alignmentScore)
    *   where list is (idx1,idx2,mismatch)
    *    where idx1 and idx2 is Option[Int] and mismatch is a Boolean indicating match
    */
  def align(pdb: PdbSeq,
            uni: UniSeq): (Seq[(Option[PdbNumber], Option[UniNumber], Boolean)],
                           Int,
                           PdbSeq,
                           UniSeq) = {
    val penalties: Map[(Char, Char), Int] = blosum
    val gapOpen = 5
    val gapExtension = 1
    val (score, pdbAl, uniAl) = GlobalPairwiseAlignment
      .globalAlignment(pdb.s, uni.s, penalties, gapOpen, gapExtension)

    assert(pdbAl.size == uniAl.size)

    def zipWithOriginalIndex(s: List[Char],
                             acc: List[Option[Int]],
                             i: Int): List[Option[Int]] =
      s match {
        case Nil                 => acc
        case x :: xs if x == '-' => zipWithOriginalIndex(xs, None :: acc, i)
        case _ :: xs             => zipWithOriginalIndex(xs, Some(i) :: acc, i + 1)
      }

    val pdbOrig = zipWithOriginalIndex(pdbAl.toList, Nil, 0).reverse
    val uniOrig = zipWithOriginalIndex(uniAl.toList, Nil, 0).reverse

    (((pdbOrig zip uniOrig) zip (pdbAl.toList zip uniAl.toList)).map {
      case ((pdb, uni), (al1, al2)) =>
        (pdb.map(i => PdbNumber(i)), uni.map(i => UniNumber(i)), al1 == al2)
    }, score, PdbSeq(pdbAl), UniSeq(uniAl))

  }

  def extractPdbIdsFromUniprot(
      uniprotKb: Map[UniId, UniProtEntry]): Seq[(UniId, PdbId, PdbChain)] =
    uniprotKb.flatMap {
      case (uniId, _) =>
        selectCandidateChains(uniId, uniprotKb).map {
          case (pdbid, pdbch, _) => (uniId, pdbid, pdbch)
        }
    }.toVector

  /** Selects the matching pdb chains the UniProtKb entries for this UniProtId
    *
    * Applies very loose filters (seqLength > 10, resolution is defined, chain length > 10)
    */
  def selectCandidateChains(
      uni: UniId,
      uniprotKb: Map[UniId, UniProtEntry]): List[(PdbId, PdbChain, UniSeq)] =
    uniprotKb
      .get(uni)
      .toList
      .filter(x => x.seqLength.isDefined && x.seqLength.get > 10)
      .flatMap { entry =>
        entry.pdbs
          .filter(x =>
            (x._2 == PdbMethod.XRay && x._3.isDefined) || (x._1: PdbId) == PdbId(
              "1JM7")) //let the BRCA nmr structure through
          .flatMap(pdb =>
            pdb._4
              .filter(chain => chain._2.map(x => x._2 - x._1 + 1).sum > 10)
              .map(chain => (pdb._1, chain._1, entry.sequence)))

      }

  def indexByPdbChain(
      uniprotKb: Seq[UniProtEntry]): Map[(PdbId, PdbChain), Seq[UniProtEntry]] =
    uniprotKb
      .flatMap(u =>
        u.pdbs.flatMap(pdb => pdb._4.map(chain => (pdb._1, chain._1) -> u)))
      .groupBy(_._1)
      .map(x => x._1 -> x._2.map(_._2))

  // Downloads from https://files.rcsb.org/download/{PDBID}.cif.gz
  def fetchCif(id: PdbId): String = {
    def decompress(compressed: Array[Byte]) =
      scala.io.Source
        .fromInputStream(
          new java.util.zip.GZIPInputStream(
            new java.io.ByteArrayInputStream(compressed)))
        .mkString

    decompress(
      scalaj.http
      // .Http(
      // s"http://www.ebi.ac.uk/pdbe/entry-files/download/${id.s.toLowerCase}.cif")
        .Http(s"https://files.rcsb.org/download/${id.s}.cif.gz")
        .timeout(connTimeoutMs = 1000 * 60 * 10, readTimeoutMs = 1000 * 60 * 10)
        .asBytes
        .body)
  }

  /** Joins up the uniprot and the pdb entries based on sequence similarity
    *
    * Fetches cif files from PDB as needed.
    */
  def joinUniProtWithPdb(uniId: UniId,
                         uniprotKb: Map[UniId, UniProtEntry]): Seq[
    (UniId,
     PdbId,
     PdbChain,
     Seq[(PdbNumber, UniNumber, PdbResidueNumber, Boolean)],
     UniSeq,
     PdbSeq,
     UniSeq,
     PdbSeq,
     Seq[(UniprotFeatureName, Set[PdbResidueNumber])])] = {

    val candidates = selectCandidateChains(uniId, uniprotKb)

    val uniprotEntry = uniprotKb(uniId)

    val uniquePdbids: Seq[PdbId] =
      candidates.map(_._1).toSeq.distinct

    if (uniId.s == "Q53SS5") {
      logger.info("Q53SS5 pdb ids: " + uniquePdbids)
    }

    val cifs: Map[PdbId, Map[PdbChain, List[(Char, PdbResidueNumber)]]] =
      uniquePdbids.flatMap { pdb =>
        val s = fetchCif(pdb)
        val lines = scala.io.Source.fromString(s).getLines.toList
        val parsedCIF = CIFContents
          .parseCIF(lines)

        parsedCIF match {
          case scala.util.Failure(e) =>
            logger.error(s"$pdb failed parsing", e)
          case _ => logger.info(s"$pdb parsing ok")
        }

        parsedCIF.toOption
          .map(_.aminoAcidSequence)
          .map(x => pdb -> x)
          .toSeq

      }.toMap

    candidates
      .filter {
        case (pdbId, pdbChain, _) =>
          val cifContainsPdb = cifs.contains(pdbId)
          val cifContainsPdbChain = cifContainsPdb && cifs(pdbId).contains(
            pdbChain)
          logger.debug(
            s"$pdbId cif contains pdb ($cifContainsPdb) and chain($cifContainsPdbChain)")
          cifContainsPdbChain
      }
      .map {
        case (pdbId, pdbChain, uniSeq) =>
          logger.debug(
            s"$pdbId $pdbChain - aligning pdb sequence with uniprot sequence")
          val pdbResidueList: Vector[(Char, PdbResidueNumber)] =
            cifs(pdbId)(pdbChain).toVector
          val pdbSeq = PdbSeq(pdbResidueList.map(_._1).mkString)
          val alignment: (Seq[(Option[PdbNumber], Option[UniNumber], Boolean)],
                          Int,
                          PdbSeq,
                          UniSeq) =
            align(pdbSeq, uniSeq)
          val mappedPdbNumbers
            : Seq[(PdbNumber, UniNumber, PdbResidueNumber, Boolean)] =
            alignment._1.filter(x => x._1.isDefined && x._2.isDefined).map {
              case (pdb, uni, matching) =>
                (pdb.get, uni.get, pdbResidueList(pdb.get.i)._2, matching)
            }
          val mapByUni = mappedPdbNumbers.groupBy(_._2)

          val featureMap: Seq[(UniprotFeatureName, Set[PdbResidueNumber])] =
            uniprotEntry.features.map {
              case (name, uniFrom, uniTo) =>
                val uniNumbers =
                  uniFrom.i until uniTo.i map (i => UniNumber(i)) toList

                (UniprotFeatureName(name),
                 uniNumbers
                   .flatMap(uni => mapByUni.get(uni).map(_.head._3))
                   .toSet)
            }

          (uniId,
           pdbId,
           pdbChain,
           mappedPdbNumbers,
           uniSeq,
           pdbSeq,
           alignment._4,
           alignment._3,
           featureMap)
      }

  }

}
