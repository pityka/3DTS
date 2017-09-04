import akka.actor._
import akka.stream._
import akka.http.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.marshalling._
import akka.event.Logging
import tasks._
import index2._
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import java.io.File
import SharedTypes._
import com.bluelabs.s3stream._
import tasks.util.TempFile

object Server {

  def makeQuery(scoresReader: IndexReader,
                cpPdbReader: IndexReader,
                q: String): ServerReturn = {
    val scores =
      if (q.isEmpty) Vector[Doc]()
      else scoresReader.getDocs(q)

    val cppdb =
      if (q.isEmpty) Vector[Doc]()
      else cpPdbReader.getDocs(q)

    (cppdb.map {
      case Doc(str) =>
        upickle.default.read[PdbUniGencodeRow](str)
    }, scores.map {
      case Doc(str) =>
        upickle.default.read[DepletionScoresByResidue](str)
    })

  }

  def start(port: Int,
            scoresIndex: ScoresIndexedByPdbId,
            cppdbIndex: CpPdbIndex)(implicit tsc: TaskSystemComponents) = {

    implicit val system = tsc.actorsystem
    implicit val mat = tsc.actorMaterializer
    import system.dispatcher
    val log = akka.event.Logging(system.eventStream, "http-server")
    log.info("Fetching index files..")
    val linkFolder = TempFile.createTempFile("links")
    linkFolder.delete
    linkFolder.mkdirs
    Future
      .sequence((scoresIndex.files ++ cppdbIndex.files).map { sf =>
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

  def httpFromFolder(indexFolder: File, port: Int)(
      implicit log: akka.event.LoggingAdapter,
      as: ActorSystem,
      mat: Materializer) = {
    import as.dispatcher

    val s3Stream = new S3StreamQueued(
      Await.result(com.bluelabs.akkaaws.AWSCredentials.default, 5 seconds).get,
      "us-west-2")

    val tableManager = TableManager(indexFolder)
    val scoresReader =
      tableManager.reader(Depletion2Pdb.ScoresByPdbIdTable)
    val cppdbReadr = tableManager.reader(JoinCPWithPdb.CpPdbTable)

    log.info("Reader ok")
    val route =
      path("query") {
        get {
          parameters("q") { queryTerm =>
            logRequest("query", Logging.InfoLevel) {
              respondWithHeader(headers.`Access-Control-Allow-Origin`.`*`) {
                complete {
                  val res = makeQuery(scoresReader, cppdbReadr, queryTerm)
                  HttpEntity(upickle.default.write(res))
                }
              }
            }
          }
        }
      } ~
        pathSingleSlash {
          logRequest("index", Logging.InfoLevel) {
            Route.seal(getFromResource("public/index.html"))
          }
        } ~
        path("browser-opt.js") {
          logRequest("browser-opt.js", Logging.InfoLevel) {
            Route.seal(getFromResource("browser-opt.js"))
          }
        } ~
        path("browser-fastopt.js") {
          logRequest("browser-fastopt.js", Logging.InfoLevel) {
            Route.seal(getFromResource("browser-fastopt.js"))
          }
        } ~
        path(RemainingPath) { segment =>
          logRequest("other", Logging.InfoLevel) {
            Route.seal(getFromResource("public/" + segment))
          }
        }

    Http().bindAndHandle(route, "0.0.0.0", port).andThen {
      case e =>
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
