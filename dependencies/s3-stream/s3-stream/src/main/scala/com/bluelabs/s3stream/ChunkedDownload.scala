package com.bluelabs.s3stream

import java.time.LocalDate

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import com.bluelabs.akkaaws.{
  AWSCredentials,
  CredentialScope,
  Signer,
  SigningKey
}

import akka.NotUsed
import akka.actor.{ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.{Attributes, ActorMaterializer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import Marshalling._

trait ChunkedDownloadSupport extends ObjectOperationsSupport with SignAndGet {

  def chunkedDownload(s3Location: S3Location,
                      chunkSize: Int = MIN_CHUNK_SIZE,
                      params: GetObjectRequest = GetObjectRequest.default)
    : Source[ByteString, NotUsed] = {
    val metadata = getMetadata(s3Location, HeadObjectRequest(params.headers))

    def retry[T](m: () => Future[T], c: Int): Future[T] =
      if (c == 0) m()
      else m().recoverWith { case e: Exception => retry(m, c - 1) }

    Source
      .fromFuture(metadata.map { metadata =>
        val size = metadata.contentLength.get
        val chunks: Seq[(Long, Long)] = 0L until size by chunkSize map (start =>
                                                                          (start,
                                                                           math
                                                                             .min(
                                                                               size,
                                                                               start + chunkSize)))
        chunks.map {
          case (start, endExcl) =>
            retry(
              () =>
                getData(s3Location,
                        params.range(headers.ByteRange(start, endExcl - 1)))
                  .runFold(ByteString())(_ ++ _),
              3)
        }.map(f => Source.fromFuture(f)).reduceLeft(_ ++ _)
      })
      .flatMapConcat(x => x)

  }

}
