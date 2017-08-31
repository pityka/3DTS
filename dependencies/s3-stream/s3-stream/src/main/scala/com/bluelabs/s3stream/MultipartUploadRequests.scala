package com.bluelabs.s3stream

import scala.concurrent.{ExecutionContext, Future}
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{Host, RawHeader}
import akka.util.ByteString

trait MultipartUploadHttpRequests extends BasicS3HttpRequests {

  def initiateMultipartUploadRequest(s3Location: S3Location,
                                     method: PostObjectRequest): HttpRequest =
    s3Request(s3Location, method, _.withQuery(Query("uploads")))

  def abortMultipartUploadRequest(s3Location: S3Location,
                                  uploadId: String): HttpRequest =
    s3Request(s3Location,
              DeleteObjectRequest,
              _.withQuery(Query("uploadId" -> uploadId)))

  def uploadPartRequest(upload: MultipartUpload,
                        partNumber: Int,
                        payload: ByteString): HttpRequest =
    s3Request(upload.s3Location,
              PutObjectRequest.default,
              _.withQuery(
                Query("partNumber" -> partNumber.toString,
                      "uploadId" -> upload.uploadId))).withEntity(payload)

  def completeMultipartUploadRequest(upload: MultipartUpload,
                                     parts: Seq[(Int, String)])(
      implicit ec: ExecutionContext): Future[HttpRequest] = {
    val payload = <CompleteMultipartUpload>
                    {
                      parts.map{case (partNumber, etag) => <Part><PartNumber>{partNumber}</PartNumber><ETag>{etag}</ETag></Part>}
                    }
                  </CompleteMultipartUpload>
    for {
      entity <- Marshal(payload).to[RequestEntity]
    } yield {
      s3Request(
        upload.s3Location,
        PostObjectRequest.default,
        _.withQuery(Query("uploadId" -> upload.uploadId))).withEntity(entity)
    }
  }

}
