package com.bluelabs.s3stream

import scala.concurrent.{ExecutionContext, Future}
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{Host, RawHeader}
import akka.util.ByteString

case class S3Location(bucket: String, key: String)

case class MultipartUpload(s3Location: S3Location, uploadId: String)

sealed trait UploadPartResponse {
  def multipartUpload: MultipartUpload
  def index: Int
}

case class SuccessfulUploadPart(multipartUpload: MultipartUpload,
                                index: Int,
                                etag: String)
    extends UploadPartResponse

case class FailedUploadPart(multipartUpload: MultipartUpload,
                            index: Int,
                            exception: Throwable)
    extends UploadPartResponse

case class FailedUpload(reasons: Seq[Throwable])
    extends Exception(reasons.head.getMessage, reasons.head)

case class CompleteMultipartUploadResult(location: Uri,
                                         bucket: String,
                                         key: String,
                                         etag: String)

case class ObjectMetadata(response: HttpResponse) {
  def ok = response.status.intValue == 200
  def contentLength = response.header[headers.`Content-Length`].map(_.length)
  def eTag = response.header[headers.`ETag`].map(_.value)
}

trait S3RequestMethod {
  def headers: Seq[HttpHeader]
  def method: HttpMethod
}

case class PostObjectRequest(headers: List[HttpHeader])
    extends S3RequestMethod {
  def method = HttpMethods.POST
  def addHeader(h: HttpHeader) = copy(headers = h :: headers)
  def metadata(key: String, value: String) =
    addHeader(RawHeader("x-amz-meta-" + key, value))
  def storageClass(st: String) =
    addHeader(RawHeader("x-amz-storage-class", st))
  def tags(tags: List[(String, String)]) =
    addHeader(
      RawHeader("x-amz-tagging",
                tags.map(x => x._1 + "=" + x._2).mkString("&")))
  def websiteRedirection(value: String) =
    addHeader(RawHeader("x-amz-website​-redirect-location", value))
  def cannedAcl(value: String) = addHeader(RawHeader("x-amz-acl", value))
  def grantRead(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-read", tpe + "=" + value))
  def grantWrite(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-write", tpe + "=" + value))
  def grantReadAcp(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-read-acp", tpe + "=" + value))
  def grantWriteAcp(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-write-acp", tpe + "=" + value))
  def grantFullControl(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-full-control", tpe + "=" + value))
  def serverSideEncryption =
    addHeader(RawHeader("x-amz-server-side-encryption", "AES256"))

}

object PostObjectRequest {
  def default = PostObjectRequest(Nil)
}

case class GetObjectRequest(headers: List[HttpHeader])
    extends S3RequestMethod {
  def method = HttpMethods.GET
  def addHeader(h: HttpHeader) = copy(headers = h :: headers)
  def range(cr: ByteRange) = addHeader(`Range`(cr))
  def ifModifiedSince(date: DateTime) =
    addHeader(RawHeader("If-Modified-Since", date.toRfc1123DateTimeString))
  def ifUnmodifiedSince(date: DateTime) =
    addHeader(RawHeader("If-Unmodified-Since", date.toRfc1123DateTimeString))
  def ifMatch(etag: String) = addHeader(RawHeader("If-Match", etag))
  def ifNoneMatch(etag: String) = addHeader(RawHeader("If-None-Match", etag))
}
object GetObjectRequest {
  def default = GetObjectRequest(Nil)
}

case class HeadObjectRequest(headers: List[HttpHeader])
    extends S3RequestMethod {
  def method = HttpMethods.HEAD
  def addHeader(h: HttpHeader) = copy(headers = h :: headers)
  def range(cr: ByteContentRange) = addHeader(`Content-Range`(cr))
  def ifModifiedSince(date: DateTime) =
    addHeader(RawHeader("If-Modified-Since", date.toRfc1123DateTimeString))
  def ifUnmodifiedSince(date: DateTime) =
    addHeader(RawHeader("If-Unmodified-Since", date.toRfc1123DateTimeString))
  def ifMatch(etag: String) = addHeader(RawHeader("If-Match", etag))
  def ifNoneMatch(etag: String) = addHeader(RawHeader("If-None-Match", etag))
}
object HeadObjectRequest {
  def default = HeadObjectRequest(Nil)
}

case class PutObjectRequest(headers: List[HttpHeader])
    extends S3RequestMethod {
  def method = HttpMethods.PUT
  def addHeader(h: HttpHeader) = copy(headers = h :: headers)
  def metadata(key: String, value: String) =
    addHeader(RawHeader("x-amz-meta-" + key, value))
  def storageClass(st: String) =
    addHeader(RawHeader("x-amz-storage-class", st))
  def tags(tags: List[(String, String)]) =
    addHeader(
      RawHeader("x-amz-tagging",
                tags.map(x => x._1 + "=" + x._2).mkString("&")))
  def websiteRedirection(value: String) =
    addHeader(RawHeader("x-amz-website​-redirect-location", value))
  def cannedAcl(value: String) = addHeader(RawHeader("x-amz-acl", value))
  def grantRead(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-read", tpe + "=" + value))
  def grantWrite(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-write", tpe + "=" + value))
  def grantReadAcp(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-read-acp", tpe + "=" + value))
  def grantWriteAcp(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-write-acp", tpe + "=" + value))
  def grantFullControl(tpe: String, value: String) =
    addHeader(RawHeader("x-amz-grant-full-control", tpe + "=" + value))
  def serverSideEncryption =
    addHeader(RawHeader("x-amz-server-side-encryption", "AES256"))

  def putCopy(source: S3Location) =
    addHeader(
      RawHeader("x-amz-copy-source", "/" + source.bucket + "/" + source.key))
}
object PutObjectRequest {
  def default = PutObjectRequest(Nil)
}

object DeleteObjectRequest extends S3RequestMethod {
  def method = HttpMethods.GET
  def headers = Nil
}
