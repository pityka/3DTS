package com.bluelabs.akkaaws

import scala.concurrent._
import scala.concurrent.duration._
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{Host, RawHeader}
import akka.util.ByteString
import akka.http.scaladsl.Http
import akka.actor._
import akka.stream._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import java.time._
import java.util.concurrent.atomic.AtomicReference

class AWSCredentialsCache(p: AWSCredentials) {
  private[this] val currentCredentials = new AtomicReference(p)

  def updateCurrent(c: AWSCredentials) = {
    currentCredentials.compareAndSet(currentCredentials.get, c)
  }

  def credentials(implicit as: ActorSystem,
                  mat: Materializer): Future[AWSCredentials] = {
    import mat.executionContext
    currentCredentials.get.refresh.map {
      case (c, updated) =>
        if (updated) { updateCurrent(c) }
        c
    }
  }
}

object AWSCredentialsCache {
  def fromInitialCredentials(c: AWSCredentials) = new AWSCredentialsCache(c)
}

sealed trait AWSCredentials {
  def accessKeyId: String
  def secretAccessKey: String

  /** Provides a valid, non-expired credentials
    *
    * Upon update, it should create a new object
    *
    * @return Future(cred,updated), where updated is true if it fetched a new set of
    * credentials
    */
  def refresh(implicit as: ActorSystem,
              am: Materializer): Future[(AWSCredentials, Boolean)]
}

case class BasicCredentials(accessKeyId: String, secretAccessKey: String)
    extends AWSCredentials {
  def refresh(implicit as: ActorSystem, am: Materializer) =
    Future.successful(this -> false)
}
case class AWSSessionCredentials(accessKeyId: String,
                                 secretAccessKey: String,
                                 sessionToken: String,
                                 expiration: Instant)
    extends AWSCredentials {

  def refresh(implicit as: ActorSystem,
              am: Materializer): Future[(AWSCredentials, Boolean)] = {
    import as.dispatcher
    val log = akka.event.Logging(as.eventStream, "awscredentials")
    val now = ZonedDateTime.now(ZoneOffset.UTC).toInstant
    val expirationMinus5Minutes = expiration.minusSeconds(60 * 5)
    if (now.isAfter(expirationMinus5Minutes)) {
      log.debug(
        "Refreshing session credentials because they will expire in 5 minutes.")
      AWSCredentials.fetchFromMetadata.map(_.get -> true)
    } else Future.successful(this -> false)
  }
}

object AWSCredentials {
  def apply(accessKeyId: String, secretAccessKey: String): BasicCredentials = {
    BasicCredentials(accessKeyId, secretAccessKey)
  }

  private def parseAWSCredentialsFile: Option[AWSCredentials] = {
    val file = System.getProperty("user.home") + "/.aws/credentials"
    if (new java.io.File(file).canRead) {
      val source = scala.io.Source.fromFile(file)
      val content = source.getLines.toVector
      source.close
      val default = content.dropWhile(line => line.trim != "[default]")
      val ac = default
        .find(_.trim.startsWith("aws_access_key_id"))
        .map(_.split("=")(1).trim)
      val sk = default
        .find(_.trim.startsWith("aws_secret_access_key"))
        .map(_.split("=")(1).trim)
      ac.flatMap { ac =>
        sk.map { sk =>
          BasicCredentials(ac, sk)
        }
      }

    } else None
  }

  def default(implicit as: ActorSystem,
              mat: Materializer): Future[Option[AWSCredentials]] = {

    val opt = {
      val access = System.getenv("AWS_ACCESS_KEY_ID")
      val secret = System.getenv("AWS_SECRET_ACCESS_KEY")
      if (access != null && secret != null)
        Some(BasicCredentials(access, secret))
      else None
    }.orElse {
      val access = System.getenv("AWS_ACCESS_KEY")
      val secret = System.getenv("AWS_SECRET_KEY")
      if (access != null && secret != null)
        Some(BasicCredentials(access, secret))
      else None
    }.orElse {
      val access = System.getProperty("aws.accessKeyId")
      val secret = System.getProperty("aws.secretKey")
      if (access != null && secret != null)
        Some(BasicCredentials(access, secret))
      else None
    }.orElse(parseAWSCredentialsFile)

    if (opt.isDefined) Future.successful(opt)
    else fetchFromMetadata

  }

  def fetchFromMetadata(
      implicit as: ActorSystem,
      mat: Materializer): Future[Option[AWSSessionCredentials]] = {
    import as.dispatcher
    val address = "169.254.169.254"
    val log = akka.event.Logging(as.eventStream, "aws-metadata")
    val request1 = HttpRequest(HttpMethods.GET)
      .withHeaders(Host(address))
      .withUri(
        Uri()
          .withHost(address)
          .withScheme("http")
          .withPath(Uri.Path("/latest/meta-data/iam/security-credentials/")))

    httpqueue
      .HttpQueue(as)
      .queue(request1)
      .flatMap(r => Unmarshal(r.entity).to[String])
      .flatMap { line =>
        val request2 = HttpRequest(HttpMethods.GET)
          .withHeaders(Host(address))
          .withUri(
            Uri()
              .withHost(address)
              .withScheme("http")
              .withPath(Uri.Path(
                "/latest/meta-data/iam/security-credentials/" + line)))

        Http()
          .singleRequest(request2)
          .flatMap(r =>
            Unmarshal(r.entity.withContentType(
              ContentTypes.`application/json`)).to[JsValue])
          .map { js =>
            val fields = js.asJsObject.fields
            val ac = fields("AccessKeyId").asInstanceOf[JsString].value
            val sk = fields("SecretAccessKey").asInstanceOf[JsString].value
            val token = fields("Token").asInstanceOf[JsString].value
            val expirationString =
              fields("Expiration").asInstanceOf[JsString].value
            Some(
              AWSSessionCredentials(
                ac,
                sk,
                token,
                ZonedDateTime.parse(expirationString).toInstant))
          }
      } recover {
      case e =>
        log.error(e, "metadata fetch fail")
        None
    }
  }

}
