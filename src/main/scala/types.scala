package sd

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import tasks.jsonitersupport._

case class Posterior(mean: Double, cdf: List[Double])
object Posterior {
  implicit val codec: JsonValueCodec[Posterior] =
    JsonCodecMaker.make[Posterior](CodecMakerConfig())
}

case class ChrPos(s: String) extends AnyVal // first 3 columns of the bed file
object ChrPos {
  implicit val codec: JsonValueCodec[ChrPos] =
    JsonCodecMaker.make[ChrPos](CodecMakerConfig())

  implicit val serde = tasks.makeSerDe[ChrPos]

}
case class FeatureName(s: String) extends AnyVal
object FeatureName {
  implicit val codec: JsonValueCodec[FeatureName] =
    JsonCodecMaker.make[FeatureName](CodecMakerConfig())
}

case class UniprotFeatureName(s: String) extends AnyVal
object UniprotFeatureName {
  implicit val codec: JsonValueCodec[UniprotFeatureName] =
    JsonCodecMaker.make[UniprotFeatureName](CodecMakerConfig())
}
case class UniId(s: String) extends AnyVal
object UniId {
  implicit val codec: JsonValueCodec[UniId] =
    JsonCodecMaker.make[UniId](CodecMakerConfig())
}
case class UniNumber(i: Int) extends AnyVal
object UniNumber {
  implicit val codec: JsonValueCodec[UniNumber] =
    JsonCodecMaker.make[UniNumber](CodecMakerConfig())
}
case class PdbId(s: String) extends AnyVal
object PdbId {
  implicit val codec: JsonValueCodec[PdbId] =
    JsonCodecMaker.make[PdbId](CodecMakerConfig())
}
case class PdbChain(s: String) extends AnyVal
object PdbChain {
  implicit val codec: JsonValueCodec[PdbChain] =
    JsonCodecMaker.make[PdbChain](CodecMakerConfig())
}
case class PdbNumber(i: Int) extends AnyVal
object PdbNumber {
  implicit val codec: JsonValueCodec[PdbNumber] =
    JsonCodecMaker.make[PdbNumber](CodecMakerConfig())
}
case class PdbResidueNumber(num: Int, insertionCode: Option[String]) {
  def toUnresolved =
    PdbResidueNumberUnresolved(num.toString + insertionCode.getOrElse(""))
}
object PdbResidueNumber {

  implicit val ordering: Ordering[PdbResidueNumber] =
    math.Ordering.by(p => (p.num, p.insertionCode.getOrElse("!")))
}
case class PdbResidueNumberUnresolved(s: String) extends AnyVal
object PdbResidueNumberUnresolved {
  implicit val codec: JsonValueCodec[PdbResidueNumberUnresolved] =
    JsonCodecMaker.make[PdbResidueNumberUnresolved](CodecMakerConfig())
}

case class UniSeq(s: String) extends AnyVal
object UniSeq {
  implicit val codec: JsonValueCodec[UniSeq] =
    JsonCodecMaker.make[UniSeq](CodecMakerConfig())
}
case class PdbSeq(s: String) extends AnyVal
object PdbSeq {
  implicit val codec: JsonValueCodec[PdbSeq] =
    JsonCodecMaker.make[PdbSeq](CodecMakerConfig())
}

case class EnsT(s: String) extends AnyVal
object EnsT {
  implicit val codec: JsonValueCodec[EnsT] =
    JsonCodecMaker.make[EnsT](CodecMakerConfig())
}
case class IndexInCodon(i: Int) extends AnyVal
object IndexInCodon {
  implicit val codec: JsonValueCodec[IndexInCodon] =
    JsonCodecMaker.make[IndexInCodon](CodecMakerConfig())
}
case class IndexInTranscript(i: Int) extends AnyVal
object IndexInTranscript {
  implicit val codec: JsonValueCodec[IndexInTranscript] =
    JsonCodecMaker.make[IndexInTranscript](CodecMakerConfig())
}
case class IndexInCds(i: Int) extends AnyVal
object IndexInCds {
  implicit val codec: JsonValueCodec[IndexInCds] =
    JsonCodecMaker.make[IndexInCds](CodecMakerConfig())
}
case class MissenseConsequences(map: Map[Char, Consequence])
case class Transcript(cdsOffset0: Int, cds: String)

sealed trait Consequence
case object Synonymous extends Consequence
case object NonSynonymous extends Consequence
case object StopGain extends Consequence
case object StopLoss extends Consequence
case object StartLoss extends Consequence
object Consequence {
  implicit val codec: JsonValueCodec[Consequence] =
    JsonCodecMaker.make[Consequence](CodecMakerConfig())

}
case class RefNuc(c: Char)

case class DepletionScoresByResidue(pdbId: String,
                                    pdbChain: String,
                                    pdbResidue: String,
                                    featureScores: DepletionRow)

object DepletionScoresByResidue {
  implicit val codec: JsonValueCodec[DepletionScoresByResidue] =
    JsonCodecMaker.make[DepletionScoresByResidue](CodecMakerConfig())
}

case class ObsNs(v: Double) extends AnyVal
object ObsNs {
  implicit val codec: JsonValueCodec[ObsNs] =
    JsonCodecMaker.make[ObsNs](CodecMakerConfig())
}
case class ExpNs(v: Double) extends AnyVal
object ExpNs {
  implicit val codec: JsonValueCodec[ExpNs] =
    JsonCodecMaker.make[ExpNs](CodecMakerConfig())
}
case class NumLoci(v: Double) extends AnyVal
object NumLoci {
  implicit val codec: JsonValueCodec[NumLoci] =
    JsonCodecMaker.make[NumLoci](CodecMakerConfig())
}

case class HeptamerIndependentIntergenicRate(v: Double) extends AnyVal
object HeptamerIndependentIntergenicRate {
  implicit val codec: JsonValueCodec[HeptamerIndependentIntergenicRate] =
    JsonCodecMaker.make[HeptamerIndependentIntergenicRate](CodecMakerConfig())
}

case class NsPostGlobalSynonymousRate(post: Posterior) extends AnyVal
object NsPostGlobalSynonymousRate {
  implicit val codec: JsonValueCodec[NsPostGlobalSynonymousRate] =
    JsonCodecMaker.make[NsPostGlobalSynonymousRate](CodecMakerConfig())
}
case class NsPostHeptamerIndependentIntergenicRate(post: Posterior)
    extends AnyVal
object NsPostHeptamerIndependentIntergenicRate {
  implicit val codec: JsonValueCodec[NsPostHeptamerIndependentIntergenicRate] =
    JsonCodecMaker.make[NsPostHeptamerIndependentIntergenicRate](
      CodecMakerConfig())
}
case class NsPostHeptamerIndependentChromosomeSpecificIntergenicRate(
    post: Posterior)
    extends AnyVal
object NsPostHeptamerIndependentChromosomeSpecificIntergenicRate {
  implicit val codec: JsonValueCodec[
    NsPostHeptamerIndependentChromosomeSpecificIntergenicRate] =
    JsonCodecMaker
      .make[NsPostHeptamerIndependentChromosomeSpecificIntergenicRate](
        CodecMakerConfig())
}
case class NsPostHeptamerSpecificIntergenicRate(post: Posterior) extends AnyVal
object NsPostHeptamerSpecificIntergenicRate {
  implicit val codec: JsonValueCodec[NsPostHeptamerSpecificIntergenicRate] =
    JsonCodecMaker.make[NsPostHeptamerSpecificIntergenicRate](
      CodecMakerConfig())
}
case class NsPostHeptamerSpecificChromosomeSpecificIntergenicRate(
    post: Posterior)
    extends AnyVal
object NsPostHeptamerSpecificChromosomeSpecificIntergenicRate {
  implicit val codec
    : JsonValueCodec[NsPostHeptamerSpecificChromosomeSpecificIntergenicRate] =
    JsonCodecMaker.make[NsPostHeptamerSpecificChromosomeSpecificIntergenicRate](
      CodecMakerConfig())
}

case class MappedPdbResidueCount(v: Int) extends AnyVal
object MappedPdbResidueCount {
  implicit val codec: JsonValueCodec[MappedPdbResidueCount] =
    JsonCodecMaker.make[MappedPdbResidueCount](CodecMakerConfig())
}

case class TotalPdbResidueCount(v: Int) extends AnyVal
object TotalPdbResidueCount {
  implicit val codec: JsonValueCodec[TotalPdbResidueCount] =
    JsonCodecMaker.make[TotalPdbResidueCount](CodecMakerConfig())
}
case class MyColor(r: Int, g: Int, b: Int)
case class ExpS(v: Double) extends AnyVal
object ExpS {
  implicit val codec: JsonValueCodec[ExpS] =
    JsonCodecMaker.make[ExpS](CodecMakerConfig())
}
case class ObsS(v: Double) extends AnyVal
object ObsS {
  implicit val codec: JsonValueCodec[ObsS] =
    JsonCodecMaker.make[ObsS](CodecMakerConfig())
}

case class FeatureKey(pdbId: PdbId,
                      pdbChain: PdbChain,
                      uniprotFeatureName: UniprotFeatureName,
                      pdbResidueMin: PdbResidueNumberUnresolved,
                      pdbResidueMax: PdbResidueNumberUnresolved) {
  override def toString =
    pdbId.s + "_" + pdbChain.s + "_" + uniprotFeatureName.s + "_" + pdbResidueMin.s + "_" + pdbResidueMax.s
}

object FeatureKey {
  implicit val codec: JsonValueCodec[FeatureKey] =
    JsonCodecMaker.make[FeatureKey](CodecMakerConfig())
}

case class DepletionScoreCDFs(
    nsPostMeanGlobalSynonymousRate: Seq[(Double, Double)],
    nsPostMeanHeptamerSpecificIntergenicRate: Seq[(Double, Double)],
    nsPostMeanHeptamerIndependentIntergenicRate: Seq[(Double, Double)],
    nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate: Seq[(Double,
                                                                     Double)],
    nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate: Seq[(Double,
                                                                        Double)]
)

object DepletionScoreCDFs {
  implicit val codec: JsonValueCodec[DepletionScoreCDFs] =
    JsonCodecMaker.make[DepletionScoreCDFs](CodecMakerConfig())

  implicit val serde = tasks.makeSerDe[DepletionScoreCDFs]
}

case class DepletionRow(
    featureKey: FeatureKey,
    obsNs: ObsNs,
    expNs: ExpNs,
    obsS: ObsS,
    expS: ExpS,
    numLoci: NumLoci,
    nsPostGlobalSynonymousRate: NsPostGlobalSynonymousRate,
    nsPostHeptamerSpecificIntergenicRate: NsPostHeptamerSpecificIntergenicRate,
    nsPostHeptamerIndependentIntergenicRate: NsPostHeptamerIndependentIntergenicRate,
    nsPostHeptamerSpecificChromosomeSpecificIntergenicRate: NsPostHeptamerSpecificChromosomeSpecificIntergenicRate,
    nsPostHeptamerIndependentChromosomeSpecificIntergenicRate: NsPostHeptamerIndependentChromosomeSpecificIntergenicRate,
    uniprotIds: Seq[UniId])

object DepletionRow {
  implicit val codec: JsonValueCodec[DepletionRow] =
    JsonCodecMaker.make[DepletionRow](CodecMakerConfig())
  implicit val serde = tasks.makeSerDe[DepletionRow]
}

case class AlignmentDetails(uniId: UniId,
                            pdbId: PdbId,
                            pdbChain: PdbChain,
                            percentIdentity: Double,
                            percentIdentityPDB: Double,
                            alignedUniSeq: UniSeq,
                            alignedPdbSeq: PdbSeq)
object AlignmentDetails {
  implicit val codec: JsonValueCodec[AlignmentDetails] =
    JsonCodecMaker.make[AlignmentDetails](CodecMakerConfig())
}

case class MappedUniprotFeature(
    uniId: UniId,
    pdbId: PdbId,
    pdbChain: PdbChain,
    featureName: UniprotFeatureName,
    residues: Set[PdbResidueNumber]
)
object MappedUniprotFeature {
  implicit val codec: JsonValueCodec[MappedUniprotFeature] =
    JsonCodecMaker.make[MappedUniprotFeature](CodecMakerConfig())
}

case class PdbUniGencodeRow(
    pdbId: PdbId,
    pdbChain: PdbChain,
    pdbResidueNumberUnresolved: PdbResidueNumberUnresolved,
    pdbSequence: PdbSeq,
    uniId: UniId,
    uniNumber: UniNumber,
    uniprotSequenceFromPdbJoin: UniSeq,
    ensT: EnsT,
    cp: ChrPos,
    indexInCodon: IndexInCodon,
    indexInTranscript: IndexInTranscript,
    missenseConsequences: MissenseConsequences,
    uniprotSequenceFromGencodeJoin: UniSeq,
    referenceNucleotide: RefNuc,
    indexInCds: IndexInCds,
    perfectMatch: Boolean)

object PdbUniGencodeRow {
  implicit val codec: JsonValueCodec[PdbUniGencodeRow] =
    JsonCodecMaker.make[PdbUniGencodeRow](CodecMakerConfig())
  implicit val serde = tasks.makeSerDe[PdbUniGencodeRow]
}

case class LigandabilityRow(uniid: UniId,
                            uninum: UniNumber,
                            data: Map[String, String])

case class MappedTranscriptToUniprot(ensT: EnsT,
                                     uniId: UniId,
                                     uniNumber: Option[UniNumber],
                                     cp: ChrPos,
                                     indexInCodon: IndexInCodon,
                                     indexInTranscript: IndexInTranscript,
                                     missenseConsequences: MissenseConsequences,
                                     uniprotSequence: UniSeq,
                                     referenceNucleotide: RefNuc,
                                     indexInCds: IndexInCds,
                                     perfectMatch: Boolean)

object MappedTranscriptToUniprot {
  implicit val codec: JsonValueCodec[MappedTranscriptToUniprot] =
    JsonCodecMaker.make[MappedTranscriptToUniprot](CodecMakerConfig())
  implicit val serde = tasks.makeSerDe[MappedTranscriptToUniprot]
}

case class PdbMethod(s: String) extends AnyVal
object PdbMethod {
  val XRay = PdbMethod("x-ray")
  val NMR = PdbMethod("nmr")
  implicit val codec: JsonValueCodec[PdbMethod] =
    JsonCodecMaker.make[PdbMethod](CodecMakerConfig())
}

case class GeneSymbol(s: String) extends AnyVal
object GeneSymbol {
  implicit val codec: JsonValueCodec[GeneSymbol] =
    JsonCodecMaker.make[GeneSymbol](CodecMakerConfig())
}

case class HeptamerOccurences(heptamer: String,
                              sampleSizes: Vector[Int],
                              variantAlleleCounts: Vector[Int],
                              heptamerCount: Int)
object HeptamerOccurences {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[HeptamerOccurences] =
    JsonCodecMaker.make[HeptamerOccurences](CodecMakerConfig())
  implicit val serde = tasks.makeSerDe[HeptamerOccurences]
}

case class HeptamerNeutralRateAndVariableCount(heptamer: String,
                                               rate: Double,
                                               countOfVariableLoci: Int,
                                               samples: Vector[Int])

object HeptamerNeutralRateAndVariableCount {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[HeptamerNeutralRateAndVariableCount] =
    JsonCodecMaker.make[HeptamerNeutralRateAndVariableCount](CodecMakerConfig())
  implicit val serde = tasks.makeSerDe[HeptamerNeutralRateAndVariableCount]
}

case class UniProtEntry(
    accessions: Seq[UniId],
    seqLength: Option[Int],
    pdbs: Seq[
      (PdbId, PdbMethod, Option[Double], Seq[(PdbChain, List[(Int, Int)])])],
    sequence: UniSeq,
    ensts: Seq[EnsT],
    features: List[(String, UniNumber, UniNumber)],
    geneNames: List[GeneName])

object UniProtEntry {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[UniProtEntry] =
    JsonCodecMaker.make[UniProtEntry](CodecMakerConfig())
}
