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

  val joinOp = EColl.join2InnerKeepGroups("feature2cpsecond-join",1, maxParallelJoins = None, partitionSize = 1024*1024*50)((r:PdbUniGencodeRow) => r.pdbId.s + "_" + r.pdbChain.s + "_" + r.pdbres.s,(r:StructuralContext.T1) => r.featureKey.pdbId.s + "_" + pdbChain + "_" + pdbResidue)

  val task =
    AsyncTask[Feature2CPInput, EColl[MappedFeatures]]("feature2cpsecond-2", 3) {

      case Feature2CPInput(
          featureContextJsDump,
          cppdbJsDump
          ) =>
        implicit ctx =>
          log.info("Start JoinFeatureWithCp")

          

          featureContextJsDump.sf.file.flatMap { featureContextLocalFile =>
            cppdbJsDump.sf.file.flatMap { cppdb =>
              val map: scala.collection.mutable.Map[String, List[String]] =
                cppdbJsDump.iterator(cppdb) { iterator =>
                  val mmap =
                    scala.collection.mutable.AnyRefMap[String, List[String]]()
                  iterator.foreach { row =>
                    val cp: ChrPos = row.cp
                    val pdbId: PdbId = row.pdbId
                    val pdbChain: PdbChain = row.pdbChain
                    val pdbres: PdbResidueNumberUnresolved =
                      row.pdbResidueNumberUnresolved
                    val k = pdbId.s + "_" + pdbChain.s + "_" + pdbres.s
                    log.debug(s"Add $k - $cp to index.")
                    mmap.get(k) match {
                      case None    => mmap.update(k, List(cp.s))
                      case Some(l) => mmap.update(k, cp.s :: l)
                    }
                  }
                  mmap
                }
              log.info("PDB -> cp map read")
              featureContextJsDump.iterator(featureContextLocalFile) {
                featureIterator =>
                  val mappedResidues: Iterator[MappedFeatures] =
                    featureIterator.flatMap {

                      case StructuralContextFeature(featureKey,
                                                    pdbResidues,
                                                    uniIds) =>
                        val joined = pdbResidues.map {
                          case (PdbChain(pdbChain),
                                PdbResidueNumberUnresolved(pdbResidue)) =>
                            log.debug(
                              s"Join $featureKey with CPs.  $pdbChain:$pdbResidue")
                            map
                              .get(featureKey.pdbId.s + "_" + pdbChain + "_" + pdbResidue)
                              .map(_.map { chrpos =>
                                (featureKey,
                                 ChrPos(chrpos),
                                 PdbChain(pdbChain),
                                 PdbResidueNumberUnresolved(pdbResidue),
                                 uniIds)
                              })

                        }.toList

                        val success = joined.count(_.isDefined)
                        val total = joined.size

                        joined.flatMap {
                          case Some(list) =>
                            list.map {
                              case (featureKey,
                                    chrpos,
                                    pdbchain,
                                    pdbnumber,
                                    uniids) =>
                                MappedFeatures(featureKey,
                                               chrpos,
                                               pdbchain,
                                               pdbnumber,
                                               uniids,
                                               MappedPdbResidueCount(success),
                                               TotalPdbResidueCount(total))
                            }
                          case None => Nil
                        }.iterator

                    }
                  JsDump.fromIterator(
                    mappedResidues,
                    featureContextJsDump.sf.name + "." + cppdbJsDump.sf.name)
              }

            }
          }
    }

}
