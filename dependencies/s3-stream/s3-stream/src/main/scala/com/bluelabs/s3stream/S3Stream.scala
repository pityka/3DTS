package com.bluelabs.s3stream

import java.time.LocalDate

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{Failure, Success}

import com.bluelabs.akkaaws.{
  AWSCredentials,
  AWSCredentialsCache,
  CredentialScope,
  Signer,
  SigningKeyProvider
}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.{Attributes, ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString

trait S3Stream
    extends ObjectOperationsSupport
    with MultipartUploadSupport
    with ChunkedDownloadSupport

class S3StreamSingle(credentials: AWSCredentials,
                     region: String = "us-east-1")(
    implicit val system: ActorSystem,
    val mat: Materializer)
    extends S3Stream {

  def singleRequest(rq: HttpRequest) = Http().singleRequest(rq)

  val signingKey =
    new SigningKeyProvider(
      AWSCredentialsCache.fromInitialCredentials(credentials),
      CredentialScope(LocalDate.now(), region, "s3"))

}

class S3StreamQueued(credentials: AWSCredentials,
                     region: String = "us-east-1")(
    implicit val system: ActorSystem,
    val mat: Materializer)
    extends S3Stream {

  def singleRequest(rq: HttpRequest) = httpqueue.HttpQueue(system).queue(rq)

  val signingKey =
    new SigningKeyProvider(
      AWSCredentialsCache.fromInitialCredentials(credentials),
      CredentialScope(LocalDate.now(), region, "s3"))

}
