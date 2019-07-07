package sd

import intervaltree._
import com.typesafe.scalalogging.StrictLogging
import akka.stream.scaladsl.{Source, Sink}
import scala.concurrent.ExecutionContext
import tasks.jsonitersupport._

case class TranscriptSupportLevel(v: Int) extends AnyVal
case class EnsE(from: Int,
                to: Int,
                chr: String,
                strand: Strand,
                ense: String,
                exonNumber: Int)
    extends Interval

object JoinGencodeToUniprot extends StrictLogging {

  val Nuc = List('A', 'T', 'G', 'C')

  sealed trait MapResult

  object MapResult {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[MapResult] =
      JsonCodecMaker.make[MapResult](sd.JsonIterConfig.config)
    implicit val serde = tasks.makeSerDe[MapResult]
  }

  def readUniProtIds(s: Source[MapResult, _])(implicit ec: ExecutionContext) =
    s.filter(_.isInstanceOf[Success])
      .mapConcat(_.asInstanceOf[Success].v.map(_.uniId).toList)
      .toMat(Sink.seq)(akka.stream.scaladsl.Keep.right)
      .mapMaterializedValue(_.map(_.toSet))

  case class Success(v: Seq[MappedTranscriptToUniprot]) extends MapResult
  case class MultipleChromosomes(exons: Seq[EnsE]) extends MapResult
  case class TranscriptSizeError(exons: Seq[EnsE], transcript: Transcript)
      extends MapResult
  case class MultipleStrands(exons: Seq[EnsE]) extends MapResult
  case class Enst2UniFailed(enst: EnsT) extends MapResult
  case class TranslationMismatch(enst: EnsT,
                                 uni: UniSeq,
                                 translated: String,
                                 transcript: Transcript)
      extends MapResult

  def mapTranscripts(gencode: Map[EnsT, (Seq[EnsE], TranscriptSupportLevel)],
                     ensembleXRefUniProt: Map[EnsT, UniId],
                     transcripts: Iterator[(EnsT, Transcript)],
                     uniprotKb: Map[UniId, UniProtEntry]) = {
    val enst2uni: Map[EnsT, UniId] = {
      val m1 = uniprotKb.flatMap(x => x._2.ensts.map(y => y -> x._1)).toMap
      m1 ++ ensembleXRefUniProt.filterNot(x => m1.contains(x._1))
    }

    enst2uni.foreach {
      case (enst, uniid) =>
        logger.debug(s"$enst $uniid")
    }

    def filterTSL(tsl: TranscriptSupportLevel) = !(tsl.v == 4 || tsl.v == 5)

    transcripts
      .filter {
        case (enst, _) =>
          val gencodeContains = gencode.contains(enst)
          val tslOK = gencodeContains && filterTSL(gencode(enst)._2)
          logger.debug(
            s"$enst kept: $tslOK, gencode contains enst: $gencodeContains")
          tslOK
      }
      .map {
        case (enst, transcript) =>
          val uni = enst2uni.get(enst)
          val result = uni
            .flatMap { uni =>
              val uniprotcontains = uniprotKb.contains(uni)
              logger.debug(s"$enst $uni UniprotKB contains: $uniprotcontains")
              uniprotKb.get(uni).map { uniEntry =>
                mapTranscript(enst,
                              gencode(enst)._1,
                              uni,
                              uniEntry.sequence,
                              transcript)
              }
            }
            .getOrElse(Enst2UniFailed(enst))

          result match {
            case Success(_) =>
              logger.info(s"$enst mapped to uniprot")
            case e =>
              logger.info(s"$enst FAILED to map to uniprot ${e.getClass}")
          }

          result
      }
  }

  def mapTranscript(enst: EnsT,
                    exons: Seq[EnsE],
                    uniprot: UniId,
                    uniSeq: UniSeq,
                    transcript: Transcript): MapResult = {

    def complement(c: Char) = c match {
      case 'A' => 'T'
      case 'G' => 'C'
      case 'T' => 'A'
      case 'C' => 'G'
    }

    if (exons.map(_.chr).distinct.size != 1) MultipleChromosomes(exons)
    else if (exons
               .map(_.size)
               .sum < transcript.cds.size + transcript.cdsOffset0)
      TranscriptSizeError(exons, transcript)
    else if (exons.map(_.strand).distinct.size != 1)
      MultipleStrands(exons)
    else {
      val translated =
        GeneticCode.translate(transcript.cds, false)

      val transcriptOffset2UniprotOffset: Option[(Map[Int, Int], Boolean)] = {
        if (translated.stripSuffix("*") == uniSeq.s)
          Some(uniSeq.s.zipWithIndex.map(x => x._2 -> x._2).toMap -> true)
        else {
          val alignment: Seq[(PdbNumber, UniNumber, Boolean)] =
            JoinUniprotWithPdb
              .align(PdbSeq(translated.filterNot(_ == '*')), uniSeq)
              ._1
              .filter(x => x._1.isDefined && x._2.isDefined)
              .map(x => (x._1.get, x._2.get, x._3))

          val qc = alignment.size >= math
            .min(translated.size, uniSeq.s.size) * 0.8 && alignment.count(_._3) >= alignment.size * 0.8
          if (qc) Some(alignment.map(x => x._1.i -> x._2.i).toMap -> false)
          else None

        }
      }

      if (transcriptOffset2UniprotOffset.isEmpty)
        TranslationMismatch(enst, uniSeq, translated, transcript)
      else {

        val (transcriptOffset2UniprotOffsetMap, perfectMatch) =
          transcriptOffset2UniprotOffset.get

        val forward = exons.head.strand == Forward

        val exonsSortedByTranscription = exons.sortBy(_.exonNumber)

        val exonStartsIndexInTranscript: List[Int] = exonsSortedByTranscription
          .foldLeft(List(0))((acc, exon) => (acc.head + exon.size) :: acc)
          .reverse

        val mapped =
          exonsSortedByTranscription.zip(exonStartsIndexInTranscript).flatMap {
            case (exon, startIndexInTranscript) =>
              0 until exon.size flatMap { exonOffset0 =>
                val currentIndexInTranscript0 = startIndexInTranscript + exonOffset0
                if (currentIndexInTranscript0 < transcript.cdsOffset0 ||
                    currentIndexInTranscript0 >= transcript.cdsOffset0 + transcript.cds.size ||
                    ((currentIndexInTranscript0 - transcript.cdsOffset0) / 3 + 1) * 3 > transcript.cds.size)
                  Nil
                else {
                  val currentIndexInCDS0 = currentIndexInTranscript0 - transcript.cdsOffset0
                  val genomicPosition0 =
                    if (forward) exon.from + exonOffset0
                    else exon.to - exonOffset0 - 1

                  val codonIndex0 = currentIndexInCDS0 / 3
                  val indexInCodon0 = currentIndexInCDS0 % 3

                  val codon =
                    transcript.cds.substring(codonIndex0 * 3,
                                             (codonIndex0 + 1) * 3)

                  val aminoAcid = translated(codonIndex0)
                  assert(GeneticCode.codon2aa(codon) == aminoAcid)

                  val refNuc =
                    if (forward) transcript.cds(currentIndexInCDS0)
                    else complement(transcript.cds(currentIndexInCDS0))

                  val missenseConsequences = Nuc
                    .filterNot(_ == refNuc)
                    .map { m =>
                      val mutatedCodon =
                        if (forward)
                          codon.toVector.updated(indexInCodon0, m).mkString
                        else
                          codon.toVector
                            .updated(indexInCodon0, complement(m))
                            .mkString

                      val mutatedAA = GeneticCode.codon2aa(mutatedCodon)
                      val consequence =
                        if (mutatedAA == aminoAcid) Synonymous
                        else if (mutatedAA == '*') StopGain
                        else if (aminoAcid == '*' && mutatedAA != '*') StopLoss
                        else if (aminoAcid == 'M' && codonIndex0 == 0 && mutatedAA != 'M')
                          StartLoss
                        else NonSynonymous
                      m -> consequence
                    }
                    .toMap

                  MappedTranscriptToUniprot(
                    enst,
                    uniprot,
                    transcriptOffset2UniprotOffsetMap
                      .get(codonIndex0)
                      .map(i => UniNumber(i)),
                    ChrPos(
                      exon.chr + "\t" + genomicPosition0 + "\t" + (genomicPosition0 + 1)),
                    IndexInCodon(indexInCodon0),
                    IndexInTranscript(currentIndexInTranscript0),
                    MissenseConsequences(missenseConsequences),
                    UniSeq(aminoAcid.toString), // this is the gencode translated amino acid
                    RefNuc(refNuc),
                    IndexInCds(currentIndexInCDS0),
                    perfectMatch
                  ) :: Nil
                }

              }

          }

        Success(mapped)
      }
    }
  }

}
