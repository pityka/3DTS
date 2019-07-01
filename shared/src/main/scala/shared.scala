package sd.shared


object AnyValPicklers {
  import upickle.default.{ReadWriter, Writer, Reader}
  def apply[S: upickle.default.ReadWriter, T](f: T => Option[S],
                                              g: S => T): ReadWriter[T] =
    ReadWriter.apply[T]((t: T) => implicitly[Writer[S]].write(f(t).get), {
      case js: upickle.Js.Value => g(implicitly[Reader[S]].read(js))
    })

}

case class Posterior(mean: Double, cdf: List[Double])
object Posterior {
  implicit val pickler = upickle.default.macroRW[Posterior]
}

// case class ChrPos(s: String) extends AnyVal // first 3 columns of the bed file
// object ChrPos {
//   implicit val pickler = AnyValPicklers(ChrPos.unapply, ChrPos.apply)
// }
// case class FeatureName(s: String) extends AnyVal
// object FeatureName {
//   implicit val pickler = AnyValPicklers(FeatureName.unapply, FeatureName.apply)
// }

case class UniprotFeatureName(s: String) extends AnyVal
object UniprotFeatureName {
  implicit val pickler =
    AnyValPicklers(UniprotFeatureName.unapply, UniprotFeatureName.apply)
}
case class UniId(s: String) extends AnyVal
object UniId {
  implicit val pickler = AnyValPicklers(UniId.unapply, UniId.apply)
}
// case class UniNumber(i: Int) extends AnyVal
// object UniNumber {
//   implicit val pickler = AnyValPicklers(UniNumber.unapply, UniNumber.apply)
// }
case class PdbId(s: String) extends AnyVal
object PdbId {
  implicit val pickler = AnyValPicklers(PdbId.unapply, PdbId.apply)
}
case class PdbChain(s: String) extends AnyVal
object PdbChain {
  implicit val pickler = AnyValPicklers(PdbChain.unapply, PdbChain.apply)
}
// case class PdbNumber(i: Int) extends AnyVal
// object PdbNumber {
//   implicit val pickler = AnyValPicklers(PdbNumber.unapply, PdbNumber.apply)
// }
// case class PdbResidueNumber(num: Int, insertionCode: Option[String]) {
//   def toUnresolved =
//     PdbResidueNumberUnresolved(num.toString + insertionCode.getOrElse(""))
// }
// object PdbResidueNumber {

//   implicit val ordering: Ordering[PdbResidueNumber] =
//     math.Ordering.by(p => (p.num, p.insertionCode.getOrElse("!")))
// }
case class PdbResidueNumberUnresolved(s: String) extends AnyVal
object PdbResidueNumberUnresolved {
  implicit val pickler =
    AnyValPicklers(PdbResidueNumberUnresolved.unapply,
                   PdbResidueNumberUnresolved.apply)
}

// case class UniSeq(s: String) extends AnyVal
// object UniSeq {
//   implicit val pickler = AnyValPicklers(UniSeq.unapply, UniSeq.apply)
// }
// case class PdbSeq(s: String) extends AnyVal
// object PdbSeq {
//   implicit val pickler = AnyValPicklers(PdbSeq.unapply, PdbSeq.apply)
// }

// case class EnsT(s: String) extends AnyVal
// object EnsT {
//   implicit val pickler = AnyValPicklers(EnsT.unapply, EnsT.apply)
// }
// case class IndexInCodon(i: Int) extends AnyVal
// object IndexInCodon {
//   implicit val pickler =
//     AnyValPicklers(IndexInCodon.unapply, IndexInCodon.apply)
// }
// case class IndexInTranscript(i: Int) extends AnyVal
// object IndexInTranscript {
//   implicit val pickler =
//     AnyValPicklers(IndexInTranscript.unapply, IndexInTranscript.apply)
// }
// case class IndexInCds(i: Int) extends AnyVal
// object IndexInCds {
//   implicit val pickler = AnyValPicklers(IndexInCds.unapply, IndexInCds.apply)
// }
// case class MissenseConsequences(map: Map[Char, Consequence])
// case class Transcript(cdsOffset0: Int, cds: String)

// sealed trait Consequence
// case object Synonymous extends Consequence
// case object NonSynonymous extends Consequence
// case object StopGain extends Consequence
// case object StopLoss extends Consequence
// case object StartLoss extends Consequence
// object Consequence {
//   import upickle.default.{ReadWriter, macroRW}
//   implicit val readWriter: ReadWriter[Consequence] =
//     macroRW[Consequence]

// }
// case class RefNuc(c: Char)

case class DepletionScoresByResidue(pdbId: String,
                                    pdbChain: String,
                                    pdbResidue: String,
                                    featureScores: DepletionRow)

object DepletionScoresByResidue {
  implicit val rw: upickle.default.ReadWriter[DepletionScoresByResidue] =
    upickle.default.macroRW[DepletionScoresByResidue]
}

case class ObsNs(v: Double) extends AnyVal
object ObsNs {
  implicit val pickler =
    AnyValPicklers(ObsNs.unapply, ObsNs.apply)
}
case class ExpNs(v: Double) extends AnyVal
object ExpNs {
  implicit val pickler =
    AnyValPicklers(ExpNs.unapply, ExpNs.apply)
}
case class NumLoci(v: Double) extends AnyVal
object NumLoci {
  implicit val pickler =
    AnyValPicklers(NumLoci.unapply, NumLoci.apply)
}

case class HeptamerIndependentIntergenicRate(v: Double) extends AnyVal
object HeptamerIndependentIntergenicRate {
  implicit val pickler =
    AnyValPicklers(HeptamerIndependentIntergenicRate.unapply, HeptamerIndependentIntergenicRate.apply)
}

case class NsPostGlobalSynonymousRate(post: Posterior) extends AnyVal
object NsPostGlobalSynonymousRate {
  implicit val pickler =
    AnyValPicklers(NsPostGlobalSynonymousRate.unapply, NsPostGlobalSynonymousRate.apply)
}
case class NsPostHeptamerIndependentIntergenicRate(post: Posterior) extends AnyVal
object NsPostHeptamerIndependentIntergenicRate {
  implicit val pickler =
    AnyValPicklers(NsPostHeptamerIndependentIntergenicRate.unapply, NsPostHeptamerIndependentIntergenicRate.apply)
}
case class NsPostHeptamerIndependentChromosomeSpecificIntergenicRate(post: Posterior) extends AnyVal
object NsPostHeptamerIndependentChromosomeSpecificIntergenicRate {
  implicit val pickler =
    AnyValPicklers(NsPostHeptamerIndependentChromosomeSpecificIntergenicRate.unapply, NsPostHeptamerIndependentChromosomeSpecificIntergenicRate.apply)
}
case class NsPostHeptamerSpecificIntergenicRate(post: Posterior) extends AnyVal
object NsPostHeptamerSpecificIntergenicRate {
  implicit val pickler =
    AnyValPicklers(NsPostHeptamerSpecificIntergenicRate.unapply, NsPostHeptamerSpecificIntergenicRate.apply)
}
case class NsPostHeptamerSpecificChromosomeSpecificIntergenicRate(post: Posterior) extends AnyVal
object NsPostHeptamerSpecificChromosomeSpecificIntergenicRate {
  implicit val pickler =
    AnyValPicklers(NsPostHeptamerSpecificChromosomeSpecificIntergenicRate.unapply, NsPostHeptamerSpecificChromosomeSpecificIntergenicRate.apply)
}

// case class MappedPdbResidueCount(v:Int) extends AnyVal
// object MappedPdbResidueCount {
//   implicit val pickler =
//     AnyValPicklers(MappedPdbResidueCount.unapply, MappedPdbResidueCount.apply)
// }

// case class TotalPdbResidueCount(v:Int) extends AnyVal
// object TotalPdbResidueCount {
//   implicit val pickler =
//     AnyValPicklers(TotalPdbResidueCount.unapply, TotalPdbResidueCount.apply)
// }
// case class MyColor(r: Int, g: Int, b: Int)
case class ExpS(v: Double) extends AnyVal
object ExpS {
  implicit val pickler =
    AnyValPicklers(ExpS.unapply, ExpS.apply)
}
case class ObsS(v: Double) extends AnyVal
object ObsS {
  implicit val pickler =
    AnyValPicklers(ObsS.unapply, ObsS.apply)
}




case class FeatureKey(pdbId: PdbId,
                       pdbChain: PdbChain,
                       uniprotFeatureName: UniprotFeatureName,
                       pdbResidueMin: PdbResidueNumberUnresolved,
                       pdbResidueMax: PdbResidueNumberUnresolved)
    {
  override def toString =
    pdbId.s + "_" + pdbChain.s + "_" + uniprotFeatureName.s + "_" + pdbResidueMin.s + "_" + pdbResidueMax.s
}

object FeatureKey {
  import upickle.default.macroRW
  implicit val rw = macroRW[FeatureKey]
}

case class DepletionScoreCDFs(
  nsPostMeanGlobalSynonymousRate : Seq[(Double,Double)],
  nsPostMeanHeptamerSpecificIntergenicRate : Seq[(Double,Double)],
  nsPostMeanHeptamerIndependentIntergenicRate : Seq[(Double,Double)],
  nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate : Seq[(Double,Double)],
  nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate : Seq[(Double,Double)]
) 

case class DepletionRow(featureKey: FeatureKey,
                        obsNs: ObsNs,
                        expNs: ExpNs,
                        obsS: ObsS,
                        expS: ExpS,
                        numLoci: NumLoci,
                        nsPostGlobalSynonymousRate: NsPostGlobalSynonymousRate,
                        nsPostHeptamerSpecificIntergenicRate: NsPostHeptamerSpecificIntergenicRate     ,   
                        nsPostHeptamerIndependentIntergenicRate: NsPostHeptamerIndependentIntergenicRate     ,   
                        nsPostHeptamerSpecificChromosomeSpecificIntergenicRate: NsPostHeptamerSpecificChromosomeSpecificIntergenicRate     ,   
                        nsPostHeptamerIndependentChromosomeSpecificIntergenicRate: NsPostHeptamerIndependentChromosomeSpecificIntergenicRate     ,   
                        uniprotIds: Seq[UniId])

object DepletionRow {
  implicit val rw = upickle.default.macroRW[DepletionRow]
}

// case class AlignmentDetails(uniId : UniId, pdbId: PdbId, pdbChain: PdbChain, percentIdentity: Double, percentIdentityPDB: Double, alignedUniSeq: UniSeq,alignedPdbSeq:PdbSeq)
// object AlignmentDetails {
//   implicit val rw = upickle.default.macroRW[AlignmentDetails]
// }

//   case class MappedUniprotFeature(
//     uniId: UniId,
//     pdbId: PdbId,
//     pdbChain: PdbChain,
//     featureName: UniprotFeatureName,
//     residues: Set[PdbResidueNumber]
//   )
//   object MappedUniprotFeature {
//     implicit val rw = upickle.default.macroRW[MappedUniprotFeature]
//   }

// object SharedTypes {

//   case class PdbUniGencodeRow(
//             pdbId: PdbId,
//             pdbChain: PdbChain,
//             pdbResidueNumberUnresolved: PdbResidueNumberUnresolved,
//             pdbSequence: PdbSeq,
//             uniId: UniId,
//             uniNumber:UniNumber,
//             uniprotSequenceFromPdbJoin:UniSeq,
//             ensT:EnsT,
//             cp: ChrPos,
//             indexInCodon:IndexInCodon,
//             indexInTranscript:IndexInTranscript,
//             missenseConsequences:MissenseConsequences,
//             uniprotSequenceFromGencodeJoin:UniSeq,
//             referenceNucleotide:RefNuc,
//             indexInCds:IndexInCds,
//             perfectMatch:Boolean)  

//   type ServerReturn = (Seq[PdbId], Seq[DepletionScoresByResidue])

// }

// case class LigandabilityRow(uniid: UniId,
//                             uninum: UniNumber,
//                             data: Map[String, String])

// case class MappedTranscriptToUniprot(ensT:EnsT,
//              uniId: UniId,
//              uniNumber:Option[UniNumber],
//              cp: ChrPos,
//              indexInCodon:IndexInCodon,
//              indexInTranscript:IndexInTranscript,
//              missenseConsequences:MissenseConsequences,
//              uniprotSequence:UniSeq,
//              referenceNucleotide:RefNuc,
//              indexInCds:IndexInCds,
//              perfectMatch:Boolean)

// object MappedTranscriptToUniprot {
//   import upickle.default.macroRW
//   implicit val rw = macroRW[MappedTranscriptToUniprot]
// }             