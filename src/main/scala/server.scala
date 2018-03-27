import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.util._
import akka.http.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.marshalling._
import akka.event.Logging
import tasks._
import index2._
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import java.io.File
import SharedTypes._
import com.bluelabs.s3stream._
import tasks.util.TempFile

object Server {

  def makeQuery(scoresReader: IndexReader,
                cpPdbReader: IndexReader,
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

    // val cppdb =
    //   if (q.isEmpty) Vector[Doc]()
    //   else cpPdbReader.getDocs(q)

    (
     //cppdb.map {
     //case Doc(str) =>
     //  upickle.default.read[PdbUniGencodeRow](str)
     //},
     pdbs,
     scores.map {
       case Doc(str) =>
         upickle.default.read[DepletionScoresByResidue](str)
     })

  }

  def asCSV(triples: Seq[
    (DepletionScoresByResidue, PdbUniGencodeRow, LigandabilityRow)]): String = {
    def asCSVRow(
        triple: (DepletionScoresByResidue, PdbUniGencodeRow, LigandabilityRow))
      : String = {
      val (scores, pdbuni, ligand) = triple
      (List(scores.pdbId,
            scores.pdbChain,
            scores.pdbResidue,
            pdbuni._5.s,
            pdbuni._6.i + 1,
            pdbuni._7.s) ++ List(
        scores.featureScores._1.toString,
        scores.featureScores._2.v,
        scores.featureScores._3.v,
        scores.featureScores._4.v,
        scores.featureScores._5.v,
        scores.featureScores._6.v,
        scores.featureScores._9.v,
        scores.featureScores._12.v
      ) ++ ligand.data
        .map(x => x._1 + ":" + x._2)).mkString(",")
    }

    val header =
      "PDBID,PDBCHAIN,PDBRES,UNIID,UNINUM,UNIAA,FEATURE,OBSNS,EXPNS,OBSS,EXPS,NUMLOCI,SCORE_global_rate,SCORE_local_rate,LIGANDABILITY"

    header + "\n" + triples.map(asCSVRow).distinct.mkString("\n")

  }

  def makeLigandibilityQuery(scoresReader: IndexReader,
                             cpPdbReader: IndexReader,
                             ligandabilityReader: IndexReader,
                             pdbQuery: String)
    : Seq[(DepletionScoresByResidue, PdbUniGencodeRow, LigandabilityRow)] = {

    val scores: Seq[DepletionScoresByResidue] =
      if (pdbQuery.isEmpty) Vector()
      else
        scoresReader.getDocs(pdbQuery).map {
          case Doc(str) =>
            upickle.default.read[DepletionScoresByResidue](str)
        }

    val pdbUni: Seq[PdbUniGencodeRow] =
      if (pdbQuery.isEmpty) Vector()
      else
        cpPdbReader.getDocs(pdbQuery).map {
          case Doc(str) =>
            upickle.default.read[PdbUniGencodeRow](str)
        }

    val uniId: Option[UniId] = pdbUni.headOption.map(_._5)

    val ligandabilityRows: Seq[LigandabilityRow] = uniId match {
      case None => Vector()
      case Some(uni) =>
        ligandabilityReader.getDocs(uni.s).map {
          case Doc(str) =>
            upickle.default.read[LigandabilityRow](str)
        }
    }

    (scores.iterator
      .flatMap { scores =>
        pdbUni.iterator.flatMap { pdbUni =>
          ligandabilityRows.flatMap { ligandability =>
            if ((scores.pdbId: String) == pdbUni._1.s &&
                (scores.pdbChain: String) == pdbUni._2.s &&
                (scores.pdbResidue: String) == pdbUni._3.s &&
                (pdbUni._5: UniId) == ligandability.uniid &&
                (pdbUni._6: UniNumber) == ligandability.uninum)
              List((scores, pdbUni, ligandability)).iterator
            else Iterator.empty
          }
        }
      } toList)
      .distinct

  }

  def start(port: Int,
            scoresIndex: ScoresIndexedByPdbId,
            cppdbIndex: CpPdbIndex,
            geneNameIndex: UniprotIndexedByGene,
            ligandabilityByUniId: Option[LigandabilityIndexedByUniId])(
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
      .sequence((scoresIndex.files ++ cppdbIndex.files ++ geneNameIndex.files ++ ligandabilityByUniId.toList
        .flatMap(_.files)).map { sf =>
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
    akka.stream.scaladsl.FileIO.fromFile(file)

  }

  def getFullDepletionScoresAsCSVStream(
      implicit ec: ExecutionContext): Source[ByteString, _] = {

    ???
    // val file = new File(
    //   dataFolder + "/depletion2pdb/full.gencode.v26lift37.annotation.gtf.gz.genome.json.gz.variationdata.json.gz.5.0.2142306777..json.gz.gencode.v26lift37.annotation.gtf.gz.genome.json.gz.-620945037.json.gz.back2pdb.json.gz")

    // Source.single(ByteString(csvHeader + "\n")) ++ akka.stream.scaladsl.FileIO
    //   .fromFile(file)
    //   .via(Compression.gunzip())
    //   .via(Framing.delimiter(ByteString("\n"),
    //                          maximumFrameLength = Int.MaxValue,
    //                          allowTruncation = true))
    //   .map(_.utf8String)
    //   .map { elem =>
    //     ByteString(
    //       csvRow(upickle.default.read[DepletionScoresByResidue](elem)) + "\n")
    //   }
  }

  def csvRow(e: DepletionScoresByResidue) = {
    List(
      e.pdbId,
      e.pdbChain,
      e.pdbResidue,
      e.featureScores._2.v,
      e.featureScores._3.v,
      e.featureScores._4.v,
      e.featureScores._5.v,
      e.featureScores._6.v,
      e.featureScores._9.v,
      e.featureScores._12.v
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
      tableManager.reader(Depletion2Pdb.ScoresByPdbIdTable)
    val cppdbReadr = tableManager.reader(JoinCPWithPdb.CpPdbTable)
    val geneNameReader = tableManager.reader(IndexUniByGeneName.UniEntryByGene)
    val ligandabilityReader =
      tableManager.reader(IndexLigandability.LigandabilityByUniId)

    log.info("Reader ok")
    val route =
      path("query") {
        get {
          parameters("q", "format".?) {
            case (queryTerm, format) =>
              logRequest("query", Logging.InfoLevel) {
                respondWithHeader(headers.`Access-Control-Allow-Origin`.`*`) {
                  complete {
                    val res = makeQuery(scoresReader,
                                        cppdbReadr,
                                        geneNameReader,
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
        path("queryligandability") {
          get {
            parameters("q", "format".?) {
              case (queryTerm, format) =>
                logRequest("query", Logging.InfoLevel) {
                  respondWithHeader(headers.`Access-Control-Allow-Origin`.`*`) {
                    complete {
                      val res = makeLigandibilityQuery(scoresReader,
                                                       cppdbReadr,
                                                       ligandabilityReader,
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
        path("pdb" / Segment) { segment =>
          logRequest("pdb", Logging.InfoLevel) {
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
            logRequest("feedback", Logging.InfoLevel) {
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
        path("depletionscores") {
          logRequest("depletionscores", Logging.InfoLevel) {
            complete {
              HttpResponse(
                entity = HttpEntity.CloseDelimited(
                  ContentTypes.`text/plain(UTF-8)`,
                  getFullDepletionScoresAsCSVStream
                )
              )
            }
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
