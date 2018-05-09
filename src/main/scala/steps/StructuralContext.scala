package sd.steps

import sd._
import scala.sys.process._
import scala.concurrent._
import tasks._
import tasks.upicklesupport._
import akka.stream.scaladsl._
import akka.util.ByteString

case class StructuralContextFromFeaturesInput(
    cifs: Map[PdbId, SharedFile],
    mappedUniprotFeatures: Set[JsDump[JoinUniprotWithPdb.T2]],
    radius: Double,
    bothSides: Boolean)

object StructuralContext {

  type T1 =
    (FeatureKey, Seq[(PdbChain, PdbResidueNumberUnresolved)], Seq[UniId])

  val taskfromFeatures =
    AsyncTask[StructuralContextFromFeaturesInput, JsDump[T1]](
      "structuralcontextfromfeatures",
      5) {

      case StructuralContextFromFeaturesInput(
          cifs,
          mappedFeatures,
          radius,
          bothSides
          ) =>
        implicit ctx =>
          log.info("Start structural features ")
          implicit val mat = ctx.components.actorMaterializer

          val source: Source[JoinUniprotWithPdb.T2, _] =
            Source(mappedFeatures).flatMapConcat { s =>
              log.info(s.toString)
              s.source
            }

          val s2: Source[(FeatureKey2,
                          Seq[(PdbChain, PdbResidueNumberUnresolved)],
                          Seq[UniId]),
                         _] = source
            .mapAsync(resourceAllocated.cpu) {
              case (uniid, pdbId, pdbchain, features) =>
                log.info(pdbId.toString)
                val cifString: Future[Option[String]] = cifs
                  .get(pdbId)
                  .map(
                    _.source
                      .runFold(ByteString())(_ ++ _)
                      .map(x => Some(x.utf8String)))
                  .getOrElse(Future.successful(None))
                cifString.map { mayCifString =>
                  mayCifString
                    .filter(_.startsWith("data_"))
                    .flatMap { cifString =>
                      CIFContents
                        .parseCIF(
                          scala.io.Source
                            .fromString(cifString)
                            .getLines
                            .toList)
                        .toOption
                        .map { cifContents =>
                          StructureContext
                            .featureFromUniprotFeatureSegmentation(
                              pdbId,
                              cifContents,
                              features.map(x => (pdbchain, x._1, x._2)).toSeq,
                              radius,
                              bothSides)
                            .map { x =>
                              (FeatureKey2(pdbId,
                                           x._1._1,
                                           x._1._2,
                                           x._1._3.toUnresolved,
                                           x._1._4.toUnresolved),
                               x._2.map(y => y._1 -> y._2.toUnresolved),
                               List(uniid))

                            }
                        }
                    }
                    .toList
                    .flatten
                }
            }
            .mapConcat(identity)

          s2.runWith(JsDump.sink(
            radius + "." + bothSides + "." + mappedFeatures.hashCode + "." + ".json.gz"))

    }

}
