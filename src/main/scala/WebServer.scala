package sd

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.util._
import akka.http.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.event.Logging
import tasks._
import index2._
import scala.concurrent.{Future}
import java.io.File
import SharedTypes._
import tasks.util.TempFile
import com.typesafe.scalalogging.StrictLogging

object Server extends StrictLogging {

  def makeQuery(scoresReader: IndexReader,
                geneNameReader: IndexReader,
                cpPdbReader: IndexReader,
                q: String): ServerReturn = {

    val pdbs: Seq[PdbId] = {
      val pdbsFromGeneNames =
        if (q.isEmpty) Vector[PdbId]()
        else
          geneNameReader
            .getDocs(q, 10000)
            .flatMap {
              case Doc(str) =>
                upickle.default.read[UniProtEntry](str).pdbs.map(_._1)
            }
            .filter { pdb =>
              val count = scoresReader.getDocCount(pdb.s)
              count > 0
            }

      val pdbsFromUniGencodeJoin =
        if (q.isEmpty) Vector()
        else
          cpPdbReader
            .getDocs(q, 10000)
            .map {
              case Doc(str) =>
                upickle.default.read[PdbUniGencodeRow](str).pdbId
            }
            .distinct
      pdbsFromGeneNames ++ pdbsFromUniGencodeJoin
    }.distinct

    logger.info(s"Query on $q returned ${pdbs.size} pdb id.")

    val scores =
      if (q.isEmpty) Vector[Doc]()
      else {
        logger.info(
          s"Scores matching query $q: ${scoresReader.getDocCount(q)}. Take first 10k.")
        scoresReader.getDocs(q, 10000)
      }

    (pdbs, scores.map {
      case Doc(str) =>
        upickle.default.read[DepletionScoresByResidue](str)
    })

  }

  def start(port: Int,
            scoresIndex: steps.ScoresIndexedByPdbId,
            cppdbIndex: steps.CpPdbIndex,
            geneNameIndex: steps.UniprotIndexedByGene,
            cdfFile: SharedFile)(implicit tsc: TaskSystemComponents) = {

    implicit val system = tsc.actorsystem
    implicit val mat = tsc.actorMaterializer
    import system.dispatcher
    val log = akka.event.Logging(system.eventStream, "http-server")
    log.info("Fetching index files..")
    val linkFolder = TempFile.createTempFile("links")
    linkFolder.delete
    linkFolder.mkdirs
    val cdfs = cdfFile.file
    Future
      .sequence((scoresIndex.files ++ cppdbIndex.files ++ geneNameIndex.files)
        .map { sf =>
          sf.file.map { file =>
            val filelinkpath =
              new java.io.File(linkFolder, sf.name)

            java.nio.file.Files.createSymbolicLink(filelinkpath.toPath,
                                                   file.toPath)

            log.info(
              "File downloaded: " + sf.name + " " + filelinkpath.getAbsolutePath)
            filelinkpath
          }
        })
      .flatMap { files =>
        cdfs.flatMap { cdfFile =>
          implicit val logging = log
          val indexFolder = files.head.getParentFile
          val cdfs =
            IOHelpers.readCDFs(cdfFile)

          log.info(
            s"Restart standalone http server with \n saturation -main sd.StandaloneHttp ${indexFolder.getAbsolutePath} $port ${cdfFile.getAbsolutePath} \n")
          httpFromFolder(indexFolder, port, cdfs)
        }
      }
  }

  val dataFolder = tasks.util.config
    .parse(com.typesafe.config.ConfigFactory.load)
    .storageURI
    .getPath

  def getData(pdb: String): Source[ByteString, _] = {

    val file = new File(dataFolder + "/pdbassembly/" + pdb + ".assembly.pdb")
    akka.stream.scaladsl.FileIO.fromPath(file.toPath)

  }

  val csvHeader = List("PDB",
                       "CHAIN",
                       "PDBRES",
                       "OBSNS",
                       "EXPNS",
                       "OBSS",
                       "EXPS",
                       "NUMLOCI",
                       "SCORE_global_rate",
                       "SCORE_local_rate").mkString(",")

  def asCSV(data: ServerReturn): String = {

    def asCSVRow(scores: DepletionScoresByResidue): String = {
      (List(scores.pdbId, scores.pdbChain, scores.pdbResidue) ++ List(
        scores.featureScores.featureKey.toString,
        scores.featureScores.obsNs.v,
        scores.featureScores.expNs.v,
        scores.featureScores.obsS.v,
        scores.featureScores.expS.v,
        scores.featureScores.numLoci.v,
        scores.featureScores.nsPostGlobalSynonymousRate.post.mean,
        scores.featureScores.nsPostHeptamerSpecificIntergenicRate.post.mean,
        scores.featureScores.nsPostHeptamerIndependentIntergenicRate.post.mean,
        scores.featureScores.nsPostHeptamerSpecificChromosomeSpecificIntergenicRate.post.mean,
        scores.featureScores.nsPostHeptamerIndependentChromosomeSpecificIntergenicRate.post.mean,
        scores.featureScores.uniprotIds.map(_.s).mkString(":")
      )).mkString(",")
    }

    val header =
      "PDBID,PDBCHAIN,PDBRES,FEATURE,OBSNS,EXPNS,OBSS_IN_CHAIN,EXPS_IN_CHAIN,NUMLOCI,nsPostGlobalSynonymousRate,nsPostHeptamerSpecificIntergenicRate,nsPostHeptamerIndependentIntergenicRate,nsPostHeptamerSpecificChromosomeSpecificIntergenicRate,nsPostHeptamerIndependentChromosomeSpecificIntergenicRate,uniprotIds"

    header + "\n" + data._2.map(asCSVRow).distinct.mkString("\n")

  }

  def httpFromFolder(indexFolder: File, port: Int, cdfs: DepletionScoreCDFs)(
      implicit log: akka.event.LoggingAdapter,
      acs: ActorSystem,
      mat: Materializer) = {
    import acs.dispatcher

    val tableManager = TableManager(indexFolder)
    val scoresReader =
      tableManager.reader(steps.DepletionToPdb.ScoresByPdbIdTable)
    val geneNameReader =
      tableManager.reader(steps.IndexUniByGeneName.UniEntryByGene)
    val cppdbReader =
      tableManager.reader(steps.JoinCPWithPdb.CpPdbTable)

    log.info("Reader ok")
    val route =
      path("query") {
        get {
          parameters(("q", "format".?)) {
            case (queryTerm, format) =>
              logRequest(("query", Logging.InfoLevel)) {
                respondWithHeader(headers.`Access-Control-Allow-Origin`.`*`) {
                  complete {
                    val res = makeQuery(scoresReader,
                                        geneNameReader,
                                        cppdbReader,
                                        queryTerm)
                    if (format.contains("csv"))
                      HttpEntity(asCSV(res))
                    else
                      HttpEntity(upickle.default.write(res))
                  }
                }
              }
          }
        }
      } ~
        pathSingleSlash {
          logRequest(("index", Logging.InfoLevel)) {
            Route.seal(getFromResource("public/index.html"))
          }
        } ~
        path("browser-opt.js") {
          logRequest(("browser-opt.js", Logging.InfoLevel)) {
            Route.seal(getFromResource("browser-opt.js"))
          }
        } ~
        path("browser-fastopt.js") {
          logRequest(("browser-fastopt.js", Logging.InfoLevel)) {
            Route.seal(getFromResource("browser-fastopt.js"))
          }
        } ~
        path("pdb" / Segment) { segment =>
          logRequest(("pdb", Logging.InfoLevel)) {
            complete {
              HttpResponse(
                entity = HttpEntity.CloseDelimited(
                  ContentTypes.`text/plain(UTF-8)`,
                  getData(segment)
                )
              )
            }
          }
        } ~
        path("feedback" / Segment) { segment =>
          post {
            logRequest(("feedback", Logging.InfoLevel)) {
              entity(as[String]) { data =>
                if (data.size < 10000) {
                  fileutils.writeToFile(
                    new java.io.File(
                      "feedback_" + java.util.UUID.randomUUID.toString),
                    segment + "\n" + data)
                  complete((StatusCodes.OK, "Thanks"))
                } else {
                  complete((StatusCodes.BadRequest, "Too large"))
                }
              }

            }

          }
        } ~
        path("cdfs") {
          get {
            complete((StatusCodes.OK, upickle.default.write(cdfs)))
          }
        } ~
        path(RemainingPath) { segment =>
          logRequest(("other", Logging.InfoLevel)) {
            Route.seal(getFromResource("public/" + segment))
          }
        }

    Http().bindAndHandle(route, "0.0.0.0", port).andThen {
      case _ =>
        log.info("http server started listening on any host on port " + port)
    }
  }
}

object StandaloneHttp extends App {
  val indexFolder = new File(args(0))
  val port = args(1).toInt
  val cdfFile = args(2)
  val cdfs = IOHelpers.readCDFs(new java.io.File(cdfFile))
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val log = akka.event.Logging(system, "http")
  Server.httpFromFolder(indexFolder, port, cdfs)

}
