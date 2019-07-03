package sd.steps

import sd._
import tasks._
import tasks.ecoll._
import tasks.jsonitersupport._

case class Feature2CPInput(featureContext: EColl[StructuralContext.T1],
                           cppdb: EColl[PdbUniGencodeRow])
object Feature2CPInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[Feature2CPInput] =
    JsonCodecMaker.make[Feature2CPInput](CodecMakerConfig())
}

object JoinFeatureWithCp {

  case class FeatureJoinedWithCp(feature: FeatureKey,
                                 locus: ChrPos,
                                 pdbChain: PdbChain,
                                 pdbResidueNumber: PdbResidueNumberUnresolved,
                                 uniprotIds: Seq[UniId],
                                 mappedPdbResidueCount: MappedPdbResidueCount,
                                 totalPdbResidueCount: TotalPdbResidueCount)

  object FeatureJoinedWithCp {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[FeatureJoinedWithCp] =
      JsonCodecMaker.make[FeatureJoinedWithCp](CodecMakerConfig())
  }

  val mappedMappedFeaturesToSupplementary =
    AsyncTask[EColl[MappedFeatures], EColl[FeatureJoinedWithCp]](
      "mappedcps-for-suppl-1",
      1) { ecoll => implicit ctx =>
      EColl.fromSource(
        ecoll.source(resourceAllocated.cpu).map { tuple =>
          FeatureJoinedWithCp(tuple.featureKey,
                              tuple.chrPos,
                              tuple.pdbChain,
                              tuple.pdbResidueNumber,
                              tuple.uniIds,
                              tuple.mappedPdbResidueCount,
                              tuple.totalPdbResidueCount)
        },
        "structuralFeatures.loci.js.gz"
      )
    }

  case class MappedFeatures(featureKey: FeatureKey,
                            chrPos: ChrPos,
                            pdbChain: PdbChain,
                            pdbResidueNumber: PdbResidueNumberUnresolved,
                            uniIds: Seq[UniId],
                            mappedPdbResidueCount: MappedPdbResidueCount,
                            totalPdbResidueCount: TotalPdbResidueCount)

  object MappedFeatures {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[MappedFeatures] =
      JsonCodecMaker.make[MappedFeatures](CodecMakerConfig())
    implicit val serde = tasks.makeSerDe[MappedFeatures]
  }

  val joinCpWithLocus = EColl.join2Inner[ChrPos, LocusVariationCountAndNumNs](
    "innerjoin-cp-locus-1",
    1,
    Some(1),
    1024 * 1024 * 50)((_: ChrPos).s, (_: LocusVariationCountAndNumNs).locus.s)

  val mappedCps = EColl.map("mappedcps-1", 1)((_: MappedFeatures).chrPos)

  val sortedCps =
    EColl.sortBy[ChrPos]("sortcps-1", 1, 1024 * 1024 * 50L)((_: ChrPos).s)

  val uniqueCps =
    EColl.distinct[ChrPos]("uniquecps-1", 1)

  case class PdbMapping(pdbId: PdbId,
                        pdbChain: PdbChain,
                        pdbRes: PdbResidueNumberUnresolved,
                        cp: ChrPos) {
    def key = pdbId.s + "_" + pdbChain.s
  }

  object PdbMapping {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[PdbMapping] =
      JsonCodecMaker.make[PdbMapping](CodecMakerConfig())
    implicit val serde = tasks.makeSerDe[PdbMapping]
  }

  case class StructureDefinition(feature: FeatureKey,
                                 pdbChain: PdbChain,
                                 pdbResidue: PdbResidueNumberUnresolved,
                                 uniIds: Seq[UniId]) {
    def key = feature.pdbId.s + "_" + pdbChain.s
  }

  object StructureDefinition {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[StructureDefinition] =
      JsonCodecMaker.make[StructureDefinition](CodecMakerConfig())
    implicit val serde = tasks.makeSerDe[StructureDefinition]
  }

  val joinFeaturesWithPdbGencodeMapping = EColl.join2LeftOuterTx(
    "feature2cp-join",
    1,
    None,
    1024 * 1024 * 50
  )(
    preTransformA = spore[PdbUniGencodeRow, List[PdbMapping]](
      (a: PdbUniGencodeRow) =>
        List(
          PdbMapping(a.pdbId, a.pdbChain, a.pdbResidueNumberUnresolved, a.cp))),
    preTransformB = spore[StructuralContext.T1, List[StructureDefinition]](
      (a: StructuralContext.T1) =>
        a.pdbResidues.toList.map {
          case (chain, residue) =>
            StructureDefinition(a.featureKey, chain, residue, a.uniprotIds)
      }),
    keyA = spore[PdbMapping, String]((a: PdbMapping) => a.key),
    keyB = spore[StructureDefinition, String]((a: StructureDefinition) => a.key),
    postTransform = spore[Seq[(Option[PdbMapping], StructureDefinition)],
                          List[MappedFeatures]](
      { (list: Seq[(Option[PdbMapping], StructureDefinition)]) =>
        val strs = list.map(_._2)
        val cps =
          list.flatMap(_._1.toSeq).map(cp => cp.pdbRes -> cp.cp).groupBy(_._1)

        assert(strs.map(_.feature).distinct == 1)
        assert(strs.map(_.pdbChain).distinct == 1)
        assert(strs.map(_.uniIds).distinct == 1)
        val StructureDefinition(featureKey, pdbChain, _, uniIds) = strs.head
        val total = strs.size
        val joined = strs.map {
          case StructureDefinition(_, _, pdbResidue, _) =>
            cps.get(pdbResidue)
        }
        val success = joined.count(_.isDefined)
        joined.flatMap {
          case Some(cps) =>
            cps.map {
              case (pdbResidue, cp) =>
                MappedFeatures(featureKey,
                               cp,
                               pdbChain,
                               pdbResidue,
                               uniIds,
                               MappedPdbResidueCount(success),
                               TotalPdbResidueCount(total))
            }
          case None => Nil
        }.toList
      }
    )
  )

  val task =
    AsyncTask[Feature2CPInput, EColl[MappedFeatures]]("feature2cpsecond-2", 3) {

      case Feature2CPInput(
          featureContext,
          cppdb
          ) =>
        implicit ctx =>
          log.info("Start JoinFeatureWithCp")

          joinFeaturesWithPdbGencodeMapping((cppdb, featureContext))(
            ResourceRequest(1, 10000))

      // featureContextJsDump.sf.file.flatMap { featureContextLocalFile =>
      //   cppdbJsDump.sf.file.flatMap { cppdb =>
      //     val map: scala.collection.mutable.Map[String, List[String]] =
      //       cppdbJsDump.iterator(cppdb) { iterator =>
      //         val mmap =
      //           scala.collection.mutable.AnyRefMap[String, List[String]]()
      //         iterator.foreach { row =>
      //           val cp: ChrPos = row.cp
      //           val pdbId: PdbId = row.pdbId
      //           val pdbChain: PdbChain = row.pdbChain
      //           val pdbres: PdbResidueNumberUnresolved =
      //             row.pdbResidueNumberUnresolved
      //           val k = pdbId.s + "_" + pdbChain.s + "_" + pdbres.s
      //           log.debug(s"Add $k - $cp to index.")
      //           mmap.get(k) match {
      //             case None    => mmap.update(k, List(cp.s))
      //             case Some(l) => mmap.update(k, cp.s :: l)
      //           }
      //         }
      //         mmap
      //       }
      //     log.info("PDB -> cp map read")
      //     featureContextJsDump.iterator(featureContextLocalFile) {
      //       featureIterator =>
      //         val mappedResidues: Iterator[MappedFeatures] =
      //           featureIterator.flatMap {

      //             case StructuralContextFeature(featureKey,
      //                                           pdbResidues,
      //                                           uniIds) =>
      //               val joined = pdbResidues.map {
      //                 case (PdbChain(pdbChain),
      //                       PdbResidueNumberUnresolved(pdbResidue)) =>
      //                   log.debug(
      //                     s"Join $featureKey with CPs.  $pdbChain:$pdbResidue")
      //                   map
      //                     .get(featureKey.pdbId.s + "_" + pdbChain + "_" + pdbResidue)
      //                     .map(_.map { chrpos =>
      //                       (featureKey,
      //                        ChrPos(chrpos),
      //                        PdbChain(pdbChain),
      //                        PdbResidueNumberUnresolved(pdbResidue),
      //                        uniIds)
      //                     })

      //               }.toList

      //               val success = joined.count(_.isDefined)
      //               val total = joined.size

      //               joined.flatMap {
      //                 case Some(list) =>
      //                   list.map {
      //                     case (featureKey,
      //                           chrpos,
      //                           pdbchain,
      //                           pdbnumber,
      //                           uniids) =>
      //                       MappedFeatures(featureKey,
      //                                      chrpos,
      //                                      pdbchain,
      //                                      pdbnumber,
      //                                      uniids,
      //                                      MappedPdbResidueCount(success),
      //                                      TotalPdbResidueCount(total))
      //                   }
      //                 case None => Nil
      //               }.iterator

      //           }
      //         JsDump.fromIterator(
      //           mappedResidues,
      //           featureContextJsDump.sf.name + "." + cppdbJsDump.sf.name)
      //     }

      //   }
      // }
    }

}
