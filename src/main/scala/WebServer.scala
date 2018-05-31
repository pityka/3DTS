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

object Server {

  def makeQuery(scoresReader: IndexReader,
                geneNameReader: IndexReader,
                q: String): ServerReturn = {

    val pdbs: Seq[PdbId] = (if (q.isEmpty) Vector[Doc]()
                            else geneNameReader.getDocs(q)).flatMap {
      case Doc(str) =>
        upickle.default.read[UniProtEntry](str).pdbs.map(_._1)
    }

    val scores =
      if (q.isEmpty) Vector[Doc]()
      else scoresReader.getDocs(q)

    (pdbs, scores.map {
      case Doc(str) =>
        upickle.default.read[DepletionScoresByResidue](str)
    })

  }

  def asCSV(
      triples: Seq[(DepletionScoresByResidue, PdbUniGencodeRow)]): String = {

    def asCSVRow(
        triple: (DepletionScoresByResidue, PdbUniGencodeRow)): String = {
      val (scores, pdbuni) = triple
      (List(scores.pdbId,
            scores.pdbChain,
            scores.pdbResidue,
            pdbuni.uniId.s,
            pdbuni.uniNumber.i + 1,
            pdbuni.uniprotSequenceFromPdbJoin.s) ++ List(
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
        scores.featureScores.uniprotIds.mkString(":")
      )).mkString(",")
    }

    val header =
      "PDBID,PDBCHAIN,PDBRES,UNIID,UNINUM,UNIAA,FEATURE,OBSNS,EXPNS,OBSS_IN_CHAIN,EXPS_IN_CHAIN,NUMLOCI,nsPostGlobalSynonymousRate,nsPostHeptamerSpecificIntergenicRate,nsPostHeptamerIndependentIntergenicRate,nsPostHeptamerSpecificChromosomeSpecificIntergenicRate,nsPostHeptamerIndependentChromosomeSpecificIntergenicRate,uniprotIds"

    header + "\n" + triples.map(asCSVRow).distinct.mkString("\n")

  }

  def start(port: Int,
            scoresIndex: steps.ScoresIndexedByPdbId,
            cppdbIndex: steps.CpPdbIndex,
            geneNameIndex: steps.UniprotIndexedByGene)(
      implicit tsc: TaskSystemComponents) = {

    implicit val system = tsc.actorsystem
    implicit val mat = tsc.actorMaterializer
    import system.dispatcher
    val log = akka.event.Logging(system.eventStream, "http-server")
    log.info("Fetching index files..")
    val linkFolder = TempFile.createTempFile("links")
    linkFolder.delete
    linkFolder.mkdirs
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
        log.info(s"Index files downloaded. $files")
        implicit val logging = log
        val indexFolder = files.head.getParentFile
        httpFromFolder(indexFolder, port)
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

  def csvRow(e: DepletionScoresByResidue) = {
    List(
      e.pdbId,
      e.pdbChain,
      e.pdbResidue
      // e.featureScores._2.v,
      // e.featureScores._3.v,
      // e.featureScores._4.v,
      // e.featureScores._5.v,
      // e.featureScores._6.v,
      // e.featureScores._9.v,
      // e.featureScores._12.v
    ).map(_.toString).mkString(",")
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

  def asCSV(data: ServerReturn) = {
    val scores = data._2
    val rows = scores
      .map(
        csvRow
      )
      .mkString("\n")

    csvHeader + "\n" + rows
  }

  def httpFromFolder(indexFolder: File, port: Int)(
      implicit log: akka.event.LoggingAdapter,
      acs: ActorSystem,
      mat: Materializer) = {
    import acs.dispatcher

    // val s3Stream = new S3StreamQueued(
    //   Await.result(com.bluelabs.akkaaws.AWSCredentials.default, 5 seconds).get,
    //   "us-west-2")

    val tableManager = TableManager(indexFolder)
    val scoresReader =
      tableManager.reader(steps.DepletionToPdb.ScoresByPdbIdTable)
    val geneNameReader =
      tableManager.reader(steps.IndexUniByGeneName.UniEntryByGene)

    log.info("Reader ok")
    val route =
      path("query") {
        get {
          parameters(("q", "format".?)) {
            case (queryTerm, format) =>
              logRequest(("query", Logging.InfoLevel)) {
                respondWithHeader(headers.`Access-Control-Allow-Origin`.`*`) {
                  complete {
                    val res = makeQuery(scoresReader, geneNameReader, queryTerm)
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
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val log = akka.event.Logging(system, "http")
  Server.httpFromFolder(indexFolder, port)

}
