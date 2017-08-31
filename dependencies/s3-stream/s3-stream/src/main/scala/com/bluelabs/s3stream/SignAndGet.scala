package com.bluelabs.s3stream

import java.time.LocalDate

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{Failure, Success}

import com.bluelabs.akkaaws.{
  AWSCredentials,
  CredentialScope,
  Signer,
  SigningKeyProvider
}

import akka.NotUsed
import akka.actor.{ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.{Attributes, Materializer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import Marshalling._

trait SignAndGet {

  implicit def system: ActorSystem
  implicit def mat: Materializer

  implicit def ec: ExecutionContext = mat.executionContext

  def signingKey: SigningKeyProvider

  def singleRequest(rq: HttpRequest): Future[HttpResponse]

  def signAndGetResponse(request: HttpRequest): Future[HttpResponse] =
    for (req <- Signer.signedRequest(request, signingKey);
         res <- singleRequest(req)) yield res

  def signAndGetAs[T](request: HttpRequest)(
      implicit um: Unmarshaller[ResponseEntity, T]): Future[T] =
    signAndGet(request).flatMap(entity => Unmarshal(entity).to[T])

  def signAndGet(request: HttpRequest): Future[ResponseEntity] =
    for (req <- Signer.signedRequest(request, signingKey);
         res <- singleRequest(req);
         t <- entityForSuccess(res)) yield t

  def entityForSuccess(resp: HttpResponse): Future[ResponseEntity] =
    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        Future.successful(entity)
      case HttpResponse(status, _, entity, _) => {
        Unmarshal(entity).to[String].flatMap {
          case err =>
            Future.failed(new Exception(err))
        }
      }
    }

  def makeCounterSource[T](f: Future[T]): Source[(T, Int), NotUsed] =
    Source
      .fromFuture(f)
      .mapConcat { case r => Stream.continually(r) }
      .zip(StreamUtils.counter(1))

}
