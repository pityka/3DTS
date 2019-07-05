package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.jsonitersupport._
import akka.stream.scaladsl._
import akka.util.ByteString
import tasks.ecoll._

case class StructuralContextFromFeaturesInput(
    cifs: Map[PdbId, SharedFile],
    mappedUniprotFeatures: Set[EColl[JoinUniprotWithPdb.T2]],
    radius: Double,
    bothSides: Boolean)

object StructuralContextFromFeaturesInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[StructuralContextFromFeaturesInput] =
    JsonCodecMaker.make[StructuralContextFromFeaturesInput](CodecMakerConfig())
}

case class StructuralContextFromFeaturesAndPdbsInput(
    pdbs: EColl[SwissModelPdbEntry],
    mappedUniprotFeatures: EColl[JoinUniprotWithPdb.T2],
    radius: Double,
    bothSides: Boolean)

object StructuralContextFromFeaturesAndPdbsInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec
    : JsonValueCodec[StructuralContextFromFeaturesAndPdbsInput] =
    JsonCodecMaker.make[StructuralContextFromFeaturesAndPdbsInput](
      CodecMakerConfig())
}

case class StructuralContextFeature(
    featureKey: FeatureKey,
    pdbResidues: Seq[(PdbChain, PdbResidueNumberUnresolved)],
    uniprotIds: Seq[UniId])

object StructuralContextFeature {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[StructuralContextFeature] =
    JsonCodecMaker.make[StructuralContextFeature](CodecMakerConfig())
  implicit val serde = tasks.makeSerDe[StructuralContextFeature]
}

object StructuralContext {

  type T1 = StructuralContextFeature

  val concatenate =
    AsyncTask[(EColl[T1], EColl[T1]), EColl[T1]](
      "concatenatedStructuralFeatures",
      1) {
      case (js1, js2) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          (js1.source(1) ++ js2.source(1))
            .runWith(EColl.sink(js1.basename + ".concat." + js2.basename))

    }

  val count =
    AsyncTask[EColl[T1], Long]("countStructuralFeatures", 1) {
      case js1 =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          js1
            .source(1)
            .fold(0L)((x, _) => x + 1)
            .runWith(Sink.head)

    }

  val fromFeaturesAndPdbStructures =
    AsyncTask[StructuralContextFromFeaturesAndPdbsInput, EColl[T1]](
      "structuralContextFromPdb-1",
      2) {
      case StructuralContextFromFeaturesAndPdbsInput(pdbs,
                                                     mappedFeatures,
                                                     radius,
                                                     bothSides) =>
        implicit ctx =>
          log.info("Start structural features from pdbs ")
          implicit val mat = ctx.components.actorMaterializer

          val source: Source[JoinUniprotWithPdb.T2, _] =
            mappedFeatures.source(resourceAllocated.cpu)

          val pdbsInMemory = pdbs
            .source(resourceAllocated.cpu)
            .statefulMapConcat { () =>
              {
                val map = scala.collection.mutable.HashMap[PdbId, String]()
                (pdbEntry: SwissModelPdbEntry) =>
                  {
                    map.update(pdbEntry.id, pdbEntry.data)
                    List(map)

                  }
              }
            }
            .runWith(Sink.last)

          pdbsInMemory.flatMap { pdbs =>
            val s2: Source[StructuralContextFeature, _] = source
              .map {
                case JoinUniprotWithPdb.T2(uniid, pdbId, pdbchain, features) =>
                  log.debug(s"$uniid $pdbId pdb:${pdbs.contains(pdbId)}")
                  val pdbString: Option[String] = pdbs
                    .get(pdbId)
                  pdbString
                    .map { pdbString =>
                      val cifContents = PdbHelper
                        .parsePdbLines(
                          scala.io.Source
                            .fromString(pdbString)
                            .getLines)

                      StructureContext
                        .featureFromUniprotFeatureSegmentation(
                          pdbId,
                          cifContents,
                          features.map(x => (pdbchain, x._1, x._2)).toSeq,
                          radius,
                          bothSides)
                        .map { x =>
                          StructuralContextFeature(
                            FeatureKey(pdbId,
                                       x._1._1,
                                       x._1._2,
                                       x._1._3.toUnresolved,
                                       x._1._4.toUnresolved),
                            x._2.map(y => y._1 -> y._2.toUnresolved),
                            List(uniid))

                        }
                    }
                    .toList
                    .flatten
              }
              .mapConcat(identity)

            s2.runWith(EColl.sink(
              radius + "." + bothSides + "." + mappedFeatures.hashCode + ".frompdb.json.gz"))
          }
    }

  val taskfromFeatures =
    AsyncTask[StructuralContextFromFeaturesInput, EColl[T1]](
      "structuralcontextfromfeatures",
      9) {

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
              log.debug(s.toString)
              s.source(resourceAllocated.cpu)
            }

          val s2 = source
            .mapAsync(resourceAllocated.cpu) {
              case JoinUniprotWithPdb.T2(uniid, pdbId, pdbchain, features) =>
                log.debug(s"$uniid $pdbId cif:${cifs.contains(pdbId)}")
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
                      val cifContents = CIFContents
                        .parseCIF(
                          scala.io.Source
                            .fromString(cifString)
                            .getLines
                            .toList)
                        .toOption

                      log.debug(
                        s"$uniid $pdbId cif parsed: ${cifContents.isDefined}")

                      cifContents
                        .map { cifContents =>
                          StructureContext
                            .featureFromUniprotFeatureSegmentation(
                              pdbId,
                              cifContents,
                              features.map(x => (pdbchain, x._1, x._2)).toSeq,
                              radius,
                              bothSides)
                            .map { x =>
                              StructuralContextFeature(
                                FeatureKey(pdbId,
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

          s2.runWith(EColl.sink(
            radius + "." + bothSides + "." + mappedFeatures.hashCode + "." + ".json.gz"))

    }

}
