object AnyValPicklers {
  import upickle.default.{ReadWriter, Writer, Reader}
  def apply[S: upickle.default.ReadWriter, T](f: T => Option[S],
                                              g: S => T): ReadWriter[T] =
    ReadWriter.apply[T]((t: T) => implicitly[Writer[S]].write(f(t).get), {
      case js: upickle.Js.Value => g(implicitly[Reader[S]].read(js))
    })

}

case class ChrPos(s: String) extends AnyVal // first 3 columns of the bed file
object ChrPos {
  implicit val pickler = AnyValPicklers(ChrPos.unapply, ChrPos.apply)
}
case class FeatureName(s: String) extends AnyVal
object FeatureName {
  implicit val pickler = AnyValPicklers(FeatureName.unapply, FeatureName.apply)
}

case class UniprotFeatureName(s: String) extends AnyVal
object UniprotFeatureName {
  implicit val pickler =
    AnyValPicklers(UniprotFeatureName.unapply, UniprotFeatureName.apply)
}
case class UniId(s: String) extends AnyVal
object UniId {
  implicit val pickler = AnyValPicklers(UniId.unapply, UniId.apply)
}
case class UniNumber(i: Int) extends AnyVal
object UniNumber {
  implicit val pickler = AnyValPicklers(UniNumber.unapply, UniNumber.apply)
}
case class PdbId(s: String) extends AnyVal
object PdbId {
  implicit val pickler = AnyValPicklers(PdbId.unapply, PdbId.apply)
}
case class PdbChain(s: String) extends AnyVal
object PdbChain {
  implicit val pickler = AnyValPicklers(PdbChain.unapply, PdbChain.apply)
}
case class PdbNumber(i: Int) extends AnyVal
object PdbNumber {
  implicit val pickler = AnyValPicklers(PdbNumber.unapply, PdbNumber.apply)
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
  implicit val pickler =
    AnyValPicklers(PdbResidueNumberUnresolved.unapply,
                   PdbResidueNumberUnresolved.apply)
}

case class UniSeq(s: String) extends AnyVal
object UniSeq {
  implicit val pickler = AnyValPicklers(UniSeq.unapply, UniSeq.apply)
}
case class PdbSeq(s: String) extends AnyVal
object PdbSeq {
  implicit val pickler = AnyValPicklers(PdbSeq.unapply, PdbSeq.apply)
}

case class EnsT(s: String) extends AnyVal
object EnsT {
  implicit val pickler = AnyValPicklers(EnsT.unapply, EnsT.apply)
}
case class IndexInCodon(i: Int) extends AnyVal
object IndexInCodon {
  implicit val pickler =
    AnyValPicklers(IndexInCodon.unapply, IndexInCodon.apply)
}
case class IndexInTranscript(i: Int) extends AnyVal
object IndexInTranscript {
  implicit val pickler =
    AnyValPicklers(IndexInTranscript.unapply, IndexInTranscript.apply)
}
case class IndexInCds(i: Int) extends AnyVal
object IndexInCds {
  implicit val pickler = AnyValPicklers(IndexInCds.unapply, IndexInCds.apply)
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
  import upickle.default.{ReadWriter, macroRW}
  implicit val readWriter: ReadWriter[Consequence] =
    macroRW[Consequence]

}
case class RefNuc(c: Char)

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
case class NsPostMean(v: Double) extends AnyVal
object NsPostMean {
  implicit val pickler =
    AnyValPicklers(NsPostMean.unapply, NsPostMean.apply)
}
case class NsPostP1(v: Double) extends AnyVal
object NsPostP1 {
  implicit val pickler =
    AnyValPicklers(NsPostP1.unapply, NsPostP1.apply)
}
case class NsPostLess10(v: Double) extends AnyVal
object NsPostLess10 {
  implicit val pickler =
    AnyValPicklers(NsPostLess10.unapply, NsPostLess10.apply)
}
case class NsPostMean2D(v: Double) extends AnyVal
object NsPostMean2D {
  implicit val pickler =
    AnyValPicklers(NsPostMean2D.unapply, NsPostMean2D.apply)
}
case class NsPostP12D(v: Double) extends AnyVal
object NsPostP12D {
  implicit val pickler =
    AnyValPicklers(NsPostP12D.unapply, NsPostP12D.apply)
}
case class NsPostLess102D(v: Double) extends AnyVal
object NsPostLess102D {
  implicit val pickler =
    AnyValPicklers(NsPostLess102D.unapply, NsPostLess102D.apply)
}
case class MyColor(r: Int, g: Int, b: Int)
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

sealed trait FeatureKey {
  def pdbId: PdbId
  def pdbChain: PdbChain
}
object FeatureKey {
  import upickle.default.macroRW
  implicit val rw = macroRW[FeatureKey1] merge macroRW[FeatureKey2]
}

case class FeatureKey1(pdbId: PdbId,
                       pdbChain: PdbChain,
                       pdbResidue: PdbResidueNumberUnresolved)
    extends FeatureKey {
  override def toString = pdbId.s + "_" + pdbChain.s + "_" + pdbResidue.s
}

case class FeatureKey2(pdbId: PdbId,
                       pdbChain: PdbChain,
                       uniprotFeatureName: UniprotFeatureName,
                       pdbResidueMin: PdbResidueNumberUnresolved,
                       pdbResidueMax: PdbResidueNumberUnresolved)
    extends FeatureKey {
  override def toString =
    pdbId.s + "_" + pdbChain.s + "_" + uniprotFeatureName.s + "_" + pdbResidueMin.s + "_" + pdbResidueMax.s
}

case class DepletionRow(_1: FeatureKey,
                        _2: ObsNs,
                        _3: ExpNs,
                        _4: ObsS,
                        _5: ExpS,
                        _6: NumLoci,
                        _7: NsPostP1,
                        _8: NsPostLess10,
                        _9: NsPostMean,
                        _10: NsPostP12D,
                        _11: NsPostLess102D,
                        _12: NsPostMean2D,
                        _13: Map[String, MyColor],
                        _14: Seq[UniId])

object DepletionRow {
  implicit val rw = upickle.default.macroRW[DepletionRow]
}

object SharedTypes {

  type PdbUniGencodeRow = (PdbId,
                           PdbChain,
                           PdbResidueNumberUnresolved,
                           PdbSeq,
                           UniId,
                           UniNumber,
                           UniSeq,
                           EnsT,
                           ChrPos,
                           IndexInCodon,
                           IndexInTranscript,
                           MissenseConsequences,
                           UniSeq,
                           RefNuc,
                           IndexInCds,
                           Boolean)

  type ServerReturn = (Seq[PdbUniGencodeRow], Seq[DepletionScoresByResidue])

}
