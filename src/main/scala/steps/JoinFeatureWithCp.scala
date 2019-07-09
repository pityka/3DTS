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
    JsonCodecMaker.make[Feature2CPInput](sd.JsonIterConfig.config)
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
      JsonCodecMaker.make[FeatureJoinedWithCp](sd.JsonIterConfig.config)
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
      JsonCodecMaker.make[MappedFeatures](sd.JsonIterConfig.config)
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
      JsonCodecMaker.make[PdbMapping](sd.JsonIterConfig.config)
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
      JsonCodecMaker.make[StructureDefinition](sd.JsonIterConfig.config)
    implicit val serde = tasks.makeSerDe[StructureDefinition]
  }

  val joinFeaturesWithPdbGencodeMapping = EColl.group2(
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
    postTransform =
      spore[Seq[(Option[PdbMapping], Option[StructureDefinition])],
            List[MappedFeatures]](
        { (list: Seq[(Option[PdbMapping], Option[StructureDefinition])]) =>
          val strs = list.flatMap(_._2.toSeq)
          val cps =
            list.flatMap(_._1.toSeq).map(cp => cp.pdbRes -> cp.cp).groupBy(_._1)

          val features = strs.groupBy(_.feature).toSeq
          val result = features.flatMap {
            case (featureKey, group) =>
              val joined = group.map {
                case StructureDefinition(_, pdbChain, pdbResidue, uniId) =>
                  (cps.get(pdbResidue), pdbChain, uniId)
              }
              val success = joined.count(_._1.isDefined)
              val total = joined.size
              joined.flatMap {
                case (Some(cps), pdbChain, uniIds) =>
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
                case _ => Nil
              }.toList
          }.toList
          result
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
          releaseResources
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
