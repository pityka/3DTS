package sd

import tasks.ecoll._
import tasks.SharedFile
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import tasks.jsonitersupport._

package object steps {

  implicit val uniIdSetCodec =
    JsonCodecMaker.make[Set[UniId]](sd.JsonIterConfig.config)
  implicit val pdbSetCodec =
    JsonCodecMaker.make[Set[PdbId]](sd.JsonIterConfig.config)
  implicit val doublePairCodec =
    JsonCodecMaker.make[Seq[(Double, Double)]](sd.JsonIterConfig.config)
  implicit val doublePairCodec2 =
    JsonCodecMaker.make[(Double, Double)](sd.JsonIterConfig.config)
  implicit val seqMappedFeaturesCodec
    : JsonValueCodec[Seq[JoinFeatureWithCp.MappedFeatures]] = JsonCodecMaker
    .make[Seq[JoinFeatureWithCp.MappedFeatures]](sd.JsonIterConfig.config)

  implicit val unidIdSerDe = tasks.makeSerDe[Set[UniId]]
  implicit val pdbMapSerDe = tasks.makeSerDe[Set[PdbId]]
  implicit val doublePairSerDe = tasks.makeSerDe[Seq[(Double, Double)]]
  implicit val doublePairSerDe2 = tasks.makeSerDe[(Double, Double)]

  implicit val serdeOptionString = tasks.makeSerDe[Option[String]]
  implicit val serdeInt = tasks.makeSerDe[Int]
  implicit val serdeLong = tasks.makeSerDe[Long]
  implicit val serdeDouble = tasks.makeSerDe[Double]
  implicit val serdeMappedFeatures =
    tasks.makeSerDe[Seq[JoinFeatureWithCp.MappedFeatures]]

  implicit val serdeMapIntInt = tasks.makeSerDe[Map[Int, Int]]

  implicit val serdeChrPosWithLocus =
    tasks.makeSerDe[(Option[ChrPos], Option[LocusVariationCountAndNumNs])]

  implicit val chrPosWithLocus
    : JsonValueCodec[(Option[ChrPos], Option[LocusVariationCountAndNumNs])] =
    JsonCodecMaker.make[(Option[ChrPos], Option[LocusVariationCountAndNumNs])](
      CodecMakerConfig())

  implicit val genomeCoverageAndGnomadLineSerDe = tasks
    .makeSerDe[(Option[GenomeCoverage], Option[JoinVariationsCore.GnomadLine])]

  implicit val seqHeptamerOccurencesCodec
    : JsonValueCodec[Seq[HeptamerOccurences]] =
    JsonCodecMaker.make[Seq[HeptamerOccurences]](sd.JsonIterConfig.config)

  implicit val joincpwithpdbCodec
    : JsonValueCodec[(Option[sd.MappedTranscriptToUniprot],
                      Option[sd.steps.JoinUniprotWithPdb.T1])] =
    JsonCodecMaker
      .make[(Option[sd.MappedTranscriptToUniprot],
             Option[sd.steps.JoinUniprotWithPdb.T1])](sd.JsonIterConfig.config)
  implicit val joincpwithpdbserde =
    tasks.makeSerDe[(Option[sd.MappedTranscriptToUniprot],
                     Option[sd.steps.JoinUniprotWithPdb.T1])]

  implicit val joincpwithlocusCodec
    : JsonValueCodec[(sd.ChrPos, sd.LocusVariationCountAndNumNs)] =
    JsonCodecMaker
      .make[(sd.ChrPos, sd.LocusVariationCountAndNumNs)](
        sd.JsonIterConfig.config)

  implicit val joincpwithlocusSerde =
    tasks.makeSerDe[(sd.ChrPos, sd.LocusVariationCountAndNumNs)]

  implicit val feature2cpJoinCodec
    : JsonValueCodec[(Option[sd.steps.JoinFeatureWithCp.PdbMapping],
                      Option[sd.steps.JoinFeatureWithCp.StructureDefinition])] =
    JsonCodecMaker
      .make[(Option[sd.steps.JoinFeatureWithCp.PdbMapping],
             Option[sd.steps.JoinFeatureWithCp.StructureDefinition])](
        CodecMakerConfig())

  implicit val feature2cpSerDe =
    tasks.makeSerDe[(Option[sd.steps.JoinFeatureWithCp.PdbMapping],
                     Option[sd.steps.JoinFeatureWithCp.StructureDefinition])]

  implicit val seqHeptamerOccurencesSerDe =
    tasks.makeSerDe[Seq[HeptamerOccurences]]
  implicit val genomeCoverageAndGnomadLineCodec: JsonValueCodec[
    (Option[GenomeCoverage], Option[JoinVariationsCore.GnomadLine])] =
    JsonCodecMaker
      .make[(Option[GenomeCoverage], Option[JoinVariationsCore.GnomadLine])](
        CodecMakerConfig())

  implicit val optionString: JsonValueCodec[Option[String]] =
    JsonCodecMaker.make[Option[String]](sd.JsonIterConfig.config)
  implicit val intCodec: JsonValueCodec[Int] =
    JsonCodecMaker.make[Int](sd.JsonIterConfig.config)
  implicit val longCodec: JsonValueCodec[Long] =
    JsonCodecMaker.make[Long](sd.JsonIterConfig.config)
  implicit val doubleCodec: JsonValueCodec[Double] =
    JsonCodecMaker.make[Double](sd.JsonIterConfig.config)
  implicit val mapIntIntCodec: JsonValueCodec[Map[Int, Int]] =
    JsonCodecMaker.make[Map[Int, Int]](sd.JsonIterConfig.config)
  implicit val pairStringIntCodec: JsonValueCodec[(String, Int)] =
    JsonCodecMaker.make[(String, Int)](sd.JsonIterConfig.config)

  implicit val pairSF: JsonValueCodec[(SharedFile, SharedFile)] =
    JsonCodecMaker.make[(SharedFile, SharedFile)](sd.JsonIterConfig.config)

  implicit val pairSFSerde = tasks.makeSerDe[(SharedFile, SharedFile)]

  implicit val pairStringIntSerDe = tasks.makeSerDe[(String, Int)]

  implicit val vecIntIntSerDe = tasks.makeSerDe[(Vector[Int], Int)]
  implicit val vecIntInt: JsonValueCodec[(Vector[Int], Int)] =
    JsonCodecMaker.make[(Vector[Int], Int)](sd.JsonIterConfig.config)

  implicit def codec[T: JsonValueCodec]: JsonValueCodec[EColl[T]] =
    JsonCodecMaker.make[EColl[T]](sd.JsonIterConfig.config)

  implicit def tup2[A: JsonValueCodec, B: JsonValueCodec]
    : JsonValueCodec[(A, B)] =
    JsonCodecMaker.make[(A, B)](sd.JsonIterConfig.config)

  implicit def tup3[A: JsonValueCodec, B: JsonValueCodec, C: JsonValueCodec]
    : JsonValueCodec[(A, B, C)] =
    JsonCodecMaker.make[(A, B, C)](sd.JsonIterConfig.config)

  implicit val codec2
    : JsonValueCodec[(Int, EColl[GenomeCoverage], Option[String])] =
    JsonCodecMaker
      .make[(Int, EColl[GenomeCoverage], Option[String])](
        sd.JsonIterConfig.config)

}
