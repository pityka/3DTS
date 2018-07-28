package sd

import sd.steps._

import tasks._
import tasks.collection._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._

class TaskRunner(implicit ts: TaskSystemComponents) extends StrictLogging {

  def run(config: Config) = {

    val contextRadius = config.getDouble("radius")

    val referenceFasta = importFile(config.getString("fasta"))
    val referenceFai = importFile(config.getString("fai"))

    val uniprotKbOriginal = importFile(config.getString("uniprotKb"))

    val uniprotKbAsJS =
      UniprotKbToJs.task(uniprotKbOriginal)(CPUMemoryRequest(1, 5000))

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

    val swissModelStructures = {
      val swissModelMetaData = importFile(
        config.getString("swissModelMetaData"))
      Swissmodel.filterMetaData(
        SwissModelMetaDataInput(swissModelMetaData, uniprotKbOriginal))(
        CPUMemoryRequest(12, 5000))
    }

    val swissModelUniPdbMap = swissModelStructures.flatMap {
      swissModelStructures =>
        Swissmodel.fakeUniprotPdbMappingFromSwissmodel(swissModelStructures)(
          CPUMemoryRequest(12, 5000))
    }

    val swissModelLinearFeatures = swissModelStructures.flatMap {
      swissModelStructures =>
        Future
          .sequence(swissModelStructures.pdbFiles.map {
            case (pdbId, pdbFile) =>
              Swissmodel
                .defineSecondaryFeaturesWithDSSP((pdbId, pdbFile))(
                  CPUMemoryRequest(1, 5000))
          })
          .map(_.toSet)
    }

    val convertedGnomadGenome = {
      val gnomadGenome = importFile(config.getString("gnomadGenome"))
      ConvertGnomadToHLI.task(GnomadData(gnomadGenome))(
        CPUMemoryRequest(1, 5000))
    }
    val convertedGnomadExome = {
      val gnomadExome = importFile(config.getString("gnomadExome"))
      ConvertGnomadToHLI.task(GnomadData(gnomadExome))(
        CPUMemoryRequest(1, 5000))
    }

    val gnomadExomeCoverage = ConvertGenomeCoverage.task(
      gnomadExomeCoverageFile -> 123136)(CPUMemoryRequest(1, 5000))
    val gnomadGenomeCoverage = ConvertGenomeCoverage.task(
      gnomadGenomeCoverageFile -> 15496)(CPUMemoryRequest(1, 5000))

    val filteredGnomadExomeCoverage = gnomadExomeCoverage.flatMap { g =>
      FilterCoverageToExome.task(FilterCoverageInput(g, gencodeGtf))(
        CPUMemoryRequest(1, 5000))
    }

    val filteredGnomadGenomeCoverage = gnomadGenomeCoverage.flatMap { g =>
      FilterCoverageToExome.task(FilterCoverageInput(g, gencodeGtf))(
        CPUMemoryRequest(1, 5000))
    }

    val gnomadWGSConvertedCoverage =
      ConvertGenomeCoverage
        .gnomadToEColl(gnomadWGSCoverage -> 15496)(CPUMemoryRequest(12, 5000))

    val gnomadWGSConvertedVCF =
      ConvertGnomadToHLI
        .gnomadToEColl(gnomadWGSVCF)(CPUMemoryRequest(12, 5000))

    val chromosomes = None :: ((1 to 22 map (i => "chr" + i)).toList)
      .map(Some(_))

    val heptamerRatesWithGlobalIntergenicRate =
      gnomadWGSConvertedCoverage.flatMap { coverage =>
        gnomadWGSConvertedVCF.flatMap { calls =>
          chromosomes.foldLeft(
            Future.successful(
              List[(sd.steps.HeptamerRates,
                    sd.HeptamerIndependentIntergenicRate,
                    Option[String])]())) {
            case (prev, chromosomeFilter) =>
              prev.flatMap { prev =>
                CountHeptamers
                  .calculateHeptamer(coverage,
                                     calls,
                                     referenceFasta,
                                     referenceFai,
                                     gencodeGtf,
                                     chromosomeFilter)
                  .map {
                    case (heptamerRates, heptamerIndependentRate) =>
                      (heptamerRates, heptamerIndependentRate, chromosomeFilter)
                  }
                  .map(result => result :: prev)
              }

          }
        }
      }

    val filteredGnomadGenome =
      convertedGnomadGenome.flatMap { gnomadGenome =>
        FilterVariantsToExome.task(
          FilterVariantsInput(gnomadGenome, "gnomad", gencodeGtf))(
          CPUMemoryRequest(1, 5000))
      }

    val filteredGnomadExome = convertedGnomadExome.flatMap { gnomadExome =>
      FilterVariantsToExome.task(
        FilterVariantsInput(gnomadExome, "gnomad", gencodeGtf))(
        CPUMemoryRequest(1, 5000))
    }

    val uniprotgencodemap = sd.steps.JoinGencodeToUniprot.task(
      GencodeUniprotInput(gencodeGtf,
                          gencodeMetadataXrefUniprot,
                          gencodeTranscripts,
                          uniprotKbOriginal))(CPUMemoryRequest(1, 30000))

    val uniprotgencodeCountUni = uniprotgencodemap
      .flatMap { uniprotgencodemap =>
        sd.steps.JoinGencodeToUniprot
          .countMappedUniprot(uniprotgencodemap)(CPUMemoryRequest(1, 5000))
      }
      .andThen {
        case count =>
          logger.info(
            s"Total number of unique uniprot ids joined in the uniprot-gencode: $count")
      }

    val uniprotgencodeCountEnsT = uniprotgencodemap
      .flatMap { uniprotgencodemap =>
        sd.steps.JoinGencodeToUniprot
          .countMappedEnst(uniprotgencodemap)(CPUMemoryRequest(1, 5000))
      }
      .andThen {
        case count =>
          logger.info(
            s"Total number of unique ensts joined in the uniprot-gencode $count")
      }

    val variationsJoined = uniprotgencodemap.flatMap { uniprotgencodemap =>
      filteredGnomadGenome.flatMap { filteredGnomadGenome =>
        filteredGnomadExome.flatMap { filteredGnomadExome =>
          filteredGnomadExomeCoverage.flatMap { exomeCov =>
            filteredGnomadGenomeCoverage.flatMap { genomeCov =>
              JoinVariations.task(
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

    val variationsJoinedEColl = variationsJoined
      .flatMap { f =>
        JoinVariations.toEColl(f)(CPUMemoryRequest(12, 60000))
      }
      .andThen {
        case scala.util.Success(variationsJoined) =>
          logger.info(s"Total coding loci: ${variationsJoined.length}")
        case _ =>
      }

    val variationCounts = for {
      variations <- variationsJoinedEColl
      _ <- JoinVariations
        .countMissense(variations)(CPUMemoryRequest(1, 5000))
        .andThen {
          case scala.util.Success(missenseCount) =>
            logger.info(s"Total missense variation: " + missenseCount)
          case _ =>
        }
      _ <- JoinVariations
        .countSynonymous(variations)(CPUMemoryRequest(1, 5000))
        .andThen {
          case scala.util.Success(synCount) =>
            logger.info(s"Total synonymous variation: " + synCount)
          case _ =>
        }
    } yield ()

    val missenseVariations = variationsJoinedEColl.flatMap { variations =>
      JoinVariations.filterMissense(variations)(CPUMemoryRequest(1, 5000))
    }

    val synonymousVariations = variationsJoinedEColl.flatMap { variations =>
      JoinVariations.filterSynonymous(variations)(CPUMemoryRequest(1, 5000))
    }

    val siteFrequencySpectrum = variationsJoinedEColl.flatMap {
      variationsJoinedEColl =>
        JoinVariations.siteFrequencySpectrum(variationsJoinedEColl)(
          CPUMemoryRequest(12, 5000))
    }

    val uniprotpdbmap = uniprotgencodemap.flatMap { uniprotgencodemap =>
      sd.steps.JoinUniprotWithPdb
        .task(UniProtPdbFullInput(uniprotKbOriginal, uniprotgencodemap))(
          CPUMemoryRequest(1, 1000))
    }

    val extractedFeatures = uniprotpdbmap.flatMap { uniprotpdbmap =>
      sd.steps.JoinUniprotWithPdb
        .extractMapppedFeatures(uniprotpdbmap)(CPUMemoryRequest((1, 12), 5000))
    }

    extractedFeatures.andThen {
      case scala.util.Success(c) =>
        logger.info("Total number of features: " + c.length)
      case scala.util.Failure(e) => logger.error("???", e)
    }

    val cppdb = uniprotgencodemap.flatMap { uniprotgencodemap =>
      uniprotpdbmap.flatMap { uniprotpdbmap =>
        JoinCPWithPdb.task(
          JoinCPWithPdbInput(uniprotgencodemap,
                             uniprotpdbmap.tables.map(_._2).sortBy(_.sf.name)))(
          CPUMemoryRequest((1, 3), 100000))
      }
    }

    val swissModelCpPdb =
      uniprotgencodemap.flatMap { uniprotgencodemap =>
        swissModelUniPdbMap.flatMap { swissModelUniPdbMap =>
          JoinCPWithPdb.task(
            JoinCPWithPdbInput(uniprotgencodemap, List(swissModelUniPdbMap)))(
            CPUMemoryRequest((1, 3), 100000))
        }
      }

    val concatenatedCpPdbJoin = swissModelCpPdb.flatMap { swissModelCpPdb =>
      cppdb.flatMap { cppdb =>
        JoinCPWithPdb.concatenate((swissModelCpPdb, cppdb))(
          CPUMemoryRequest(1, 5000))
      }
    }

    val cppdbindex = concatenatedCpPdbJoin.flatMap { cppdb =>
      JoinCPWithPdb.indexCpPdb(cppdb)(CPUMemoryRequest(1, 60000))
    }

    val mappableUniprotIds = uniprotgencodemap.flatMap { uniprotgencodemap =>
      MappableUniprot.task(uniprotgencodemap)(CPUMemoryRequest(1, 1000))
    }

    val cifs = mappableUniprotIds.flatMap { mappedUniprotIds =>
      AssemblyToPdb.fetchCif(
        Assembly2PdbInput(uniprotKbOriginal, mappedUniprotIds))(
        CPUMemoryRequest((1, 8), 3000))
    }

    val assemblies = cifs.flatMap { cifs =>
      AssemblyToPdb.assembly(cifs)(CPUMemoryRequest((1, 16), 3000))
    }

    val scores = {
      val radius = contextRadius

      def joinFeatureWithCp(
          features: Future[JsDump[steps.StructuralContext.T1]],
          cppdb: Future[JsDump[SharedTypes.PdbUniGencodeRow]]) = {
        val feature2cp = cppdb.flatMap { cppdb =>
          features.flatMap { features =>
            JoinFeatureWithCp.task(
              Feature2CPInput(featureContext = features, cppdb = cppdb))(
              CPUMemoryRequest(1, 120000))
          }
        }

        val feature2cpEcoll = feature2cp.flatMap { f =>
          JoinFeatureWithCp.toEColl(f)(CPUMemoryRequest(12, 5000))
        }

        (features, feature2cpEcoll)
      }

      def makeSwissModelFeatures = {
        val features = swissModelStructures.flatMap { pdbs =>
          swissModelLinearFeatures.flatMap { features =>
            StructuralContext.fromFeaturesAndPdbStructures(
              StructuralContextFromFeaturesAndPdbsInput(
                pdbs = pdbs.pdbFiles,
                mappedUniprotFeatures = features,
                radius = radius,
                bothSides = true))(CPUMemoryRequest((1, 12), 1000))
          }
        }

        joinFeatureWithCp(features, swissModelCpPdb)
      }

      def makeStructuralFeatures(includeBothSidesOfPlane: Boolean) = {
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

        joinFeatureWithCp(features, cppdb)

      }

      def makeDepletionScores(
          features2cpEcoll: Future[EColl[JoinFeatureWithCp.MappedFeatures]]) =
        features2cpEcoll.flatMap { feature2cp =>
          variationsJoinedEColl.flatMap { variationsJoined =>
            heptamerRatesWithGlobalIntergenicRate.flatMap { listOfRates =>
              val (chromosomeIndependentHeptamer,
                   chromosomeIndependentHeptamerIndependent,
                   _) =
                listOfRates.find {
                  case (_, _, chromosomeFilter) => chromosomeFilter.isEmpty
                }.get

              val chromosomeSpecificHeptamerRates = listOfRates.collect {
                case (heptamerRates, _, Some(chromosomeFilter)) =>
                  (chromosomeFilter, heptamerRates)
              }

              val chromosomeSpecificHeptamerIndependentRates =
                listOfRates.collect {
                  case (_, heptamerIndependentRate, Some(chromosomeFilter)) =>
                    (chromosomeFilter, heptamerIndependentRate)
                }

              depletion3d.computeDepletionScores(
                variationsJoined,
                feature2cp,
                fasta = referenceFasta,
                fai = referenceFai,
                heptamerNeutralRates = chromosomeIndependentHeptamer,
                heptamerIndependentIntergenicRate =
                  chromosomeIndependentHeptamerIndependent,
                chromosomeSpecificHeptamerRates =
                  chromosomeSpecificHeptamerRates.toMap,
                chromosomeSpecificHeptamerIndependentRates =
                  chromosomeSpecificHeptamerIndependentRates.toMap
              )
            }
          }
        }

      def extractAggregates(tag: String, scores: Future[EColl[DepletionRow]]) =
        scores.flatMap { scores =>
          for {
            _ <- depletion3d
              .uniquePdbIds(scores)(CPUMemoryRequest(1, 5000))
              .andThen {
                case scala.util.Success(set) =>
                  logger.info(s"$tag Number of unique scored pdbs: ${set.size}")
                case _ =>
              }
            _ <- depletion3d
              .uniqueUniprotIds(scores)(CPUMemoryRequest(1, 5000))
              .andThen {
                case scala.util.Success(set) =>
                  logger.info(
                    s"$tag Number of unique scored uniprot: ${set.size}")
                case _ =>
              }
          } yield ()
        }

      val (cifFeatures, cifFeature2cpEcoll) = makeStructuralFeatures(
        includeBothSidesOfPlane = true)

      val (swissModelFeatures, swissModelFeature2cpEcoll) =
        makeSwissModelFeatures

      val cifDepletionScores = makeDepletionScores(cifFeature2cpEcoll)

      val cifAggregates = extractAggregates("PDB", cifDepletionScores)

      val supplementaryFeature2Cp =
        cifFeature2cpEcoll.flatMap { cifFeature2cpEcoll =>
          swissModelFeature2cpEcoll.flatMap { swissModelFeature2cpEcoll =>
            JoinFeatureWithCp.mappedMappedFeaturesToSupplementary(
              cifFeature2cpEcoll ++ swissModelFeature2cpEcoll)(
              CPUMemoryRequest(1, 5000))
          }
        }

      val swissModelDepletionScores = makeDepletionScores(
        swissModelFeature2cpEcoll)

      val swissModelAggregates =
        extractAggregates("swissmodel", swissModelDepletionScores)

      val uniqueMappedCps = (for {
        swissModelFeature2cp <- swissModelFeature2cpEcoll
        cifFeature2cp <- cifFeature2cpEcoll
        concat = swissModelFeature2cp ++ cifFeature2cp
        cps <- JoinFeatureWithCp.mappedCps(concat)(CPUMemoryRequest(1, 5000))
        sorted <- JoinFeatureWithCp.sortedCps(cps)(CPUMemoryRequest(1, 5000))
        unique <- JoinFeatureWithCp.uniqueCps(sorted)(CPUMemoryRequest(1, 5000))
      } yield unique).andThen {
        case scala.util.Success(unique) =>
          logger.info(
            "Total number of loci mapped to any protein : " + unique.length)
      }

      val uniqueMappedMissenseVariations = for {
        cps <- uniqueMappedCps
        missense <- missenseVariations
        synonymous <- synonymousVariations
        joinedMissense <- JoinFeatureWithCp
          .joinCpWithLocus((cps, missense))(CPUMemoryRequest(1, 100000))
          .andThen {
            case scala.util.Success(joinedMissense) =>
              logger.info(
                "Total number of mapped missense: " + joinedMissense.length)
          }
        joinedSynonymous <- JoinFeatureWithCp
          .joinCpWithLocus((cps, synonymous))(CPUMemoryRequest(1, 100000))
          .andThen {
            case scala.util.Success(joinedSyn) =>
              logger.info(
                "Total number of mapped synonymous: " + joinedSyn.length)
          }
      } yield ()

      val concatenatedDepletionScores = for {
        c1 <- cifDepletionScores
        c2 <- swissModelDepletionScores
      } yield c1 ++ c2

      val cdfs = concatenatedDepletionScores
        .flatMap { s =>
          depletion3d.computeCDFs(s)
        }
        .flatMap { cdf =>
          depletion3d.cdfs2file(cdf)(CPUMemoryRequest(1, 5000))
        }

      val concatenatedFeatures = for {
        c1 <- swissModelFeatures
        c2 <- cifFeatures
        r <- StructuralContext.concatenate((c1, c2))(CPUMemoryRequest(1, 5000))
      } yield r

      val featureCount = concatenatedFeatures.flatMap { feat =>
        StructuralContext.count(feat)(CPUMemoryRequest(1, 5000))
      }

      featureCount.andThen {
        case scala.util.Success(d) =>
          logger.info("Total number of structural featrue: " + d)
        case e =>
          println("???" + e)
      }

      val scores2pdb = concatenatedDepletionScores.flatMap { scores =>
        concatenatedFeatures.flatMap { features =>
          DepletionToPdb.task(
            Depletion2PdbInput(
              scores,
              features
            ))(CPUMemoryRequest(1, 10000))
        }
      }

      val repartitionedScores = concatenatedDepletionScores.flatMap { ds =>
        depletion3d.repartition(ds)(CPUMemoryRequest((1, 12), 5000))
      }

      val indexedScores = scores2pdb.flatMap { scores =>
        DepletionToPdb.indexByPdbId(scores)(CPUMemoryRequest(1, 20000))
      }

      val server = indexedScores.flatMap { index =>
        cppdbindex.flatMap { cppdb =>
          uniprotByGene.flatMap { uniprotByGene =>
            cdfs.flatMap { cdfs =>
              Server.start(8080, index, cppdb, uniprotByGene, cdfs)
            }
          }
        }
      }

      val archive = concatenatedFeatures.flatMap { structuralFeatures =>
        repartitionedScores.flatMap { scores =>
          concatenatedCpPdbJoin.flatMap { cppdb =>
            supplementaryFeature2Cp.flatMap { supplementaryFeature2Cp =>
              TarArchive.archiveSharedFiles(TarArchiveInput(
                Map(
                  "scores.js.gz" -> scores.files.head,
                  "structuralFeatures.js.gz" -> structuralFeatures.sf,
                  "gencodeUniprotPdb.js.gz" -> cppdb.sf,
                  "structuralFeatures.loci.js.gz" -> supplementaryFeature2Cp.files.head
                ),
                "archive.tar"
              ))(CPUMemoryRequest(1, 5000))
            }
          }
        }
      }

      val archiveForWebServer =
        for {
          index <- indexedScores
          cppdb <- cppdbindex
          uniprotByGene <- uniprotByGene
          cdfs <- cdfs
          assemblies <- assemblies
          swissModelPdbs <- swissModelStructures
          tar <- {
            val pdbPaths =
              (swissModelPdbs.pdbFiles ++ assemblies.pdbFiles)
                .map {
                  case (PdbId(pdbId), sf) =>
                    s"pdbassembly/$pdbId.assembly.pdb" -> sf
                }
            val cdfPath = Map("cdf/cdf.txt" -> cdfs)
            val indexPaths =
              (index.files ++ cppdb.files ++ uniprotByGene.files)
                .map { sf =>
                  (("index/" + sf.name) -> sf)
                }

            TarArchive.archiveSharedFiles(
              TarArchiveInput(
                pdbPaths ++ cdfPath ++ indexPaths,
                "indexarchive.tar"
              ))(CPUMemoryRequest(1, 5000))
          }
        } yield tar

      List(
        // cifDepletionScores
        // swissModelDepletionScores
        extractedFeatures,
        featureCount,
        uniqueMappedCps,
        uniqueMappedMissenseVariations,
        cdfs,
        indexedScores,
        uniprotByGene,
        server,
        repartitionedScores,
        cifAggregates,
        swissModelAggregates,
        concatenatedCpPdbJoin,
        archive,
        archiveForWebServer
      )
    }

    Future.sequence(
      List(
        uniprotgencodeCountEnsT,
        uniprotgencodeCountUni,
        variationCounts,
        cppdbindex,
        uniprotgencodemap,
        cppdb,
        uniprotpdbmap,
        variationsJoined,
        uniprotgencodemap,
        assemblies,
        siteFrequencySpectrum
      ) ++ scores)

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

  val config = ConfigFactory.load()

  withTaskSystem { implicit ts =>
    Await.result(new TaskRunner().run(config), atMost = 168 hours)
    println("Pipeline done. Blocking indefinitely to keep the server up.")
    while (true) {
      Thread.sleep(Long.MaxValue)
    }
  }

}
