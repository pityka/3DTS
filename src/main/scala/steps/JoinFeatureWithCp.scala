package sd.steps

import sd._
import tasks._
import tasks.collection._
import tasks.upicklesupport._

case class Feature2CPInput(featureContext: JsDump[StructuralContext.T1],
                           cppdb: JsDump[SharedTypes.PdbUniGencodeRow])

object JoinFeatureWithCp {

  type MappedFeatures =
    (FeatureKey, ChrPos, PdbChain, PdbResidueNumberUnresolved, Seq[UniId])

  val toEColl =
    AsyncTask[JsDump[MappedFeatures], EColl[MappedFeatures]](
      "convertfeature2cp-ecoll-1",
      1) {
      case js =>
        implicit ctx =>
          EColl.fromSource(js.source,
                           js.sf.name,
                           1024 * 1024 * 50,
                           parallelism = resourceAllocated.cpu)

    }

  val task =
    AsyncTask[Feature2CPInput, JsDump[MappedFeatures]]("feature2cpsecond-2", 2) {

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
                    log.info(s"Add $k - $cp to index.")
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

                      case (featureKey, pdbResidues, uniIds) =>
                        pdbResidues.iterator.flatMap {
                          case (PdbChain(pdbChain),
                                PdbResidueNumberUnresolved(pdbResidue)) =>
                            log.debug(
                              s"Join $featureKey with CPs.  $pdbChain:$pdbResidue")
                            map
                              .get(featureKey.pdbId.s + "_" + pdbChain + "_" + pdbResidue)
                              .toList
                              .iterator
                              .flatMap(_.map { chrpos =>
                                (featureKey,
                                 ChrPos(chrpos),
                                 PdbChain(pdbChain),
                                 PdbResidueNumberUnresolved(pdbResidue),
                                 uniIds)
                              })

                        }

                    }
                  JsDump.fromIterator(
                    mappedResidues,
                    featureContextJsDump.sf.name + "." + cppdbJsDump.sf.name)
              }

            }
          }
    }

}