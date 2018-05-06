import fileutils._
import stringsplit._

import IOHelpers._
import MathHelpers._
import Model._

import tasks._
import tasks.collection._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.net._
import com.typesafe.config._

import scala.collection.JavaConverters._

class TaskRunner(implicit ts: TaskSystemComponents) {

  def run(config: Config) = {

    val referenceFasta = importFile(config.getString("fasta"))
    val referenceFai = importFile(config.getString("fai"))

    val ligandability =
      if (config.hasPath("ligandability"))
        Some(importFile(config.getString("ligandability")))
      else None

    val ligandabilityJs =
      ligandability.map(LigandabilityCsvToJs.task(_)(CPUMemoryRequest(1, 5000)))

    val indexedLigandability = ligandabilityJs
      .map(_.flatMap { ligandabilityJs =>
        IndexLigandability
          .task(ligandabilityJs)(CPUMemoryRequest(1, 5000))
          .map(Some(_))
      })
      .getOrElse(Future.successful(None))

    val uniprotKbOriginal = importFile(config.getString("uniprotKb"))

    val uniprotKbAsJS =
      UniprotKb2Js.task(uniprotKbOriginal)(CPUMemoryRequest(1, 5000))

    val uniprotByGene = uniprotKbAsJS.flatMap { uniprotKbAsJS =>
      IndexUniByGeneName.task(uniprotKbAsJS)(CPUMemoryRequest(1, 5000))
    }

    val gencodeGtf = importFile(config.getString("gencodeGTF"))

    val gencodeTranscripts = importFile(config.getString("gencodeTranscripts"))

    val gencodeMetadataXrefUniprot = importFile(
      config.getString("gencodeMetadataXrefUniprot"))

    val gnomadExomeCoverageFile: SharedFile = importFile(
      config.getString("gnomadExomeCoverage"))

    val gnomadGenomeCoverageFile: SharedFile = importFile(
      config.getString("gnomadGenomeCoverage"))

    val gnomadWGSCoverage: List[SharedFile] =
      config
        .getStringList("gnomadWGSCoverage")
        .asScala
        .toList
        .map(importFile.apply)
    val gnomadWGSVCF: List[GnomadData] =
      config
        .getStringList("gnomadWGSVCF")
        .asScala
        .toList
        .map(s => importFile(s))
        .map(GnomadData(_))

    val convertedGnomadGenome = {
      val gnomadGenome = importFile(config.getString("gnomadGenome"))
      ConvertGnomad2HLI.task(GnomadData(gnomadGenome))(
        CPUMemoryRequest(1, 5000))
    }
    val convertedGnomadExome = {
      val gnomadExome = importFile(config.getString("gnomadExome"))
      ConvertGnomad2HLI.task(GnomadData(gnomadExome))(CPUMemoryRequest(1, 5000))
    }

    val gnomadExomeCoverage = ConvertGenomeCoverage.task(
      gnomadExomeCoverageFile -> 123136)(CPUMemoryRequest(1, 5000))
    val gnomadGenomeCoverage = ConvertGenomeCoverage.task(
      gnomadGenomeCoverageFile -> 15496)(CPUMemoryRequest(1, 5000))

    val filteredGnomadExomeCoverage = gnomadExomeCoverage.flatMap { g =>
      FilterCoverage.task(FilterCoverageInput(g, gencodeGtf))(
        CPUMemoryRequest(1, 5000))
    }

    val filteredGnomadGenomeCoverage = gnomadGenomeCoverage.flatMap { g =>
      FilterCoverage.task(FilterCoverageInput(g, gencodeGtf))(
        CPUMemoryRequest(1, 5000))
    }

    val gnomadWGSConvertedCoverage =
      ConvertGenomeCoverage
        .gnomadToEColl(gnomadWGSCoverage -> 15496)(CPUMemoryRequest(12, 5000))

    val gnomadWGSConvertedVCF =
      ConvertGnomad2HLI
        .gnomadToEColl(gnomadWGSVCF)(CPUMemoryRequest(12, 5000))

    val heptamerRatesWithGlobalIntergenicRate =
      gnomadWGSConvertedCoverage.flatMap { coverage =>
        gnomadWGSConvertedVCF.flatMap { calls =>
          CountHeptamers.calculateHeptamer(coverage,
                                           calls,
                                           referenceFasta,
                                           referenceFai,
                                           gencodeGtf)
        }
      }

    val filteredGnomadGenome =
      convertedGnomadGenome.flatMap { gnomadGenome =>
        FilterGnomad.task(
          FilterGnomadInput(gnomadGenome, "gnomad", gencodeGtf))(
          CPUMemoryRequest(1, 5000))
      }

    val filteredGnomadExome = convertedGnomadExome.flatMap { gnomadExome =>
      FilterGnomad.task(FilterGnomadInput(gnomadExome, "gnomad", gencodeGtf))(
        CPUMemoryRequest(1, 5000))
    }

    val uniprotgencodemap = gencodeUniprot.task(
      GencodeUniprotInput(gencodeGtf,
                          gencodeMetadataXrefUniprot,
                          gencodeTranscripts,
                          uniprotKbOriginal))(CPUMemoryRequest(1, 30000))

    val variationsJoined = uniprotgencodemap.flatMap { uniprotgencodemap =>
      filteredGnomadGenome.flatMap { filteredGnomadGenome =>
        filteredGnomadExome.flatMap { filteredGnomadExome =>
          filteredGnomadExomeCoverage.flatMap { exomeCov =>
            filteredGnomadGenomeCoverage.flatMap { genomeCov =>
              joinVariations.task(
                JoinVariationsInput(filteredGnomadExome.f,
                                    filteredGnomadGenome.f,
                                    uniprotgencodemap,
                                    exomeCov,
                                    genomeCov,
                                    gencodeGtf))(CPUMemoryRequest(1, 60000))
            }
          }
        }
      }
    }

    val variationsJoinedEColl = variationsJoined.flatMap { f =>
      joinVariations.toEColl(f)(CPUMemoryRequest(12, 60000))
    }

    val uniprotpdbmap = uniprotgencodemap.flatMap { uniprotgencodemap =>
      UniProtPdb.task(
        UniProtPdbFullInput(uniprotKbOriginal, uniprotgencodemap))(
        CPUMemoryRequest(1, 1000))
    }

    val cppdb = uniprotgencodemap.flatMap { uniprotgencodemap =>
      uniprotpdbmap.flatMap { uniprotpdbmap =>
        JoinCPWithPdb.task(
          JoinCPWithPdbInput(uniprotgencodemap,
                             uniprotpdbmap.tables.map(_._2).sortBy(_.sf.name)))(
          CPUMemoryRequest((1, 3), 60000))
      }
    }

    val cppdbindex = cppdb.flatMap { cppdb =>
      JoinCPWithPdb.indexCpPdb(cppdb)(CPUMemoryRequest(1, 60000))
    }

    val simplecppdb = cppdb.flatMap { cppdb =>
      ProjectCpPdb.task(cppdb)(CPUMemoryRequest(1, 5000))
    }

    val mappableUniprotIds = uniprotgencodemap.flatMap { uniprotgencodemap =>
      MappableUniprot.task(uniprotgencodemap)(CPUMemoryRequest(1, 1000))
    }

    val cifs = mappableUniprotIds.flatMap { mappedUniprotIds =>
      Assembly2Pdb.fetchCif(
        Assembly2PdbInput(uniprotKbOriginal, mappedUniprotIds))(
        CPUMemoryRequest((1, 8), 3000))
    }

    val assemblies = cifs.flatMap { cifs =>
      Assembly2Pdb.assembly(cifs)(CPUMemoryRequest((1, 16), 3000))
    }

    val scores = {
      val radius = 5d

      def makeFeatures(includeBothSidesOfPlane: Boolean) = {
        val features = uniprotpdbmap.flatMap { uniprotpdbmap =>
          cifs.flatMap { cifs =>
            StructuralContext.taskfromFeatures(
              StructuralContextFromFeaturesInput(
                cifs = cifs.cifFiles,
                mappedUniprotFeatures = uniprotpdbmap.tables.map(_._3).toSet,
                radius = radius,
                bothSides = includeBothSidesOfPlane))(
              CPUMemoryRequest((1, 12), 1000))
          }
        }

        val feature2cp = cppdb.flatMap { cppdb =>
          features.flatMap { features =>
            Feature2CPSecond.task(
              Feature2CPSecondInput(featureContext = features, cppdb = cppdb))(
              CPUMemoryRequest(1, 60000))
          }
        }

        val feature2cpEcoll = feature2cp.flatMap { f =>
          Feature2CPSecond.toEColl(f)(CPUMemoryRequest(12, 5000))
        }

        (features, feature2cpEcoll)

      }

      val (features, feature2cpEcoll) = makeFeatures(
        includeBothSidesOfPlane = true)

      def makeDepletionScores(
          features2cpEcoll: Future[EColl[Feature2CPSecond.MappedFeatures]]) =
        features2cpEcoll.flatMap { feature2cp =>
          variationsJoinedEColl.flatMap { variationsJoined =>
            heptamerRatesWithGlobalIntergenicRate.flatMap {
              case (heptamerRates, globalIntergenicRate) =>
                depletion3d.computeDepletionScores(variationsJoined,
                                                   feature2cp,
                                                   fasta = referenceFasta,
                                                   fai = referenceFai,
                                                   heptamerNeutralRates =
                                                     heptamerRates,
                                                   globalIntergenicRate =
                                                     globalIntergenicRate)
            }
          }
        }

      val depletionScores = makeDepletionScores(feature2cpEcoll)

      val scores2pdb = depletionScores.flatMap { scores =>
        features.flatMap { features =>
          Depletion2Pdb.task(
            Depletion2PdbInput(
              scores,
              features
            ))(CPUMemoryRequest(1, 10000))
        }
      }

      val indexedScores = scores2pdb.flatMap { scores =>
        Depletion2Pdb.indexByPdbId(scores)(CPUMemoryRequest(1, 20000))
      }

      // var server = indexedScores.flatMap { index =>
      //   cppdbindex.flatMap { cppdb =>
      //     uniprotByGene.flatMap { uniprotByGene =>
      //       indexedLigandability.flatMap { indexedLigandability =>
      //         Server.start(8080,
      //                      index,
      //                      cppdb,
      //                      uniprotByGene,
      //                      indexedLigandability)
      //       }
      //     }
      //   }
      // }

      List(indexedScores)
    }

    Future.sequence(
      List(cppdbindex,
           uniprotgencodemap,
           cppdb,
           simplecppdb,
           uniprotpdbmap,
           variationsJoined,
           uniprotgencodemap,
           assemblies) ++ scores)

  }
}

object await {
  def apply[T](f: Future[T]) = Await.result(f, 60 minutes)
}

object importFile {
  def apply(s: String)(implicit ce: TaskSystemComponents) =
    if (s.startsWith("http") || s.startsWith("s3"))
      await(SharedFile(tasks.util.Uri(s)))
    else await(SharedFile(new java.io.File(s), new java.io.File(s).getName))
}

object ProteinDepletion extends App {

  System.setProperty("org.apache.commons.logging.Log",
                     "org.apache.commons.logging.impl.NoOpLog");

  //val startServer = args(0).toBoolean

  val config = ConfigFactory.load()

  withTaskSystem { implicit ts =>
    val result =
      Await.result(new TaskRunner().run(config), atMost = 168 hours)
    //if (startServer) {
    println("Pipeline done. Blocking indefinitely to keep the server up.")
    while (true) {
      Thread.sleep(Long.MaxValue)
    }
  //}
  }

}
