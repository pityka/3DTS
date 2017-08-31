package com.bluelabs.s3stream

import scala.concurrent.{ExecutionContext, Future}
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{Host, RawHeader}
import akka.util.ByteString

trait BasicS3HttpRequests {

  def s3Request(s3Location: S3Location,
                method: S3RequestMethod,
                uriFn: (Uri => Uri) = identity): HttpRequest =
    HttpRequest(method.method)
      .withHeaders(method.headers: _*)
      .addHeader(Host(requestHost(s3Location)))
      .withUri(uriFn(requestUri(s3Location)))

  def getRequest(s3Location: S3Location,
                 method: GetObjectRequest): HttpRequest =
    s3Request(s3Location, method)

  def deleteRequest(s3Location: S3Location): HttpRequest =
    s3Request(s3Location, DeleteObjectRequest)

  def headRequest(s3Location: S3Location,
                  method: HeadObjectRequest): HttpRequest =
    s3Request(s3Location, method)

  def putRequest(s3Location: S3Location,
                 method: PutObjectRequest,
                 payload: ByteString): HttpRequest =
    s3Request(s3Location, method).withEntity(payload)

  def putCopyRequest(from: S3Location,
                     to: S3Location,
                     method: PutObjectRequest): HttpRequest =
    s3Request(to, method.putCopy(to))

  def requestHost(s3Location: S3Location): Uri.Host =
    Uri.Host(s"${s3Location.bucket}.s3.amazonaws.com")

  def requestUri(s3Location: S3Location): Uri =
    Uri(s"/${s3Location.key}")
      .withHost(requestHost(s3Location))
      .withScheme("https")
}
