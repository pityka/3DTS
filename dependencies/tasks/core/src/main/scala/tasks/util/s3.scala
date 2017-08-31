package tasks.util

import com.bluelabs.akkaaws._
import com.bluelabs.s3stream._
import akka.actor._
import akka.stream._
import scala.concurrent._
import scala.concurrent.duration._

object S3Helpers {

  def credentials(implicit as: ActorSystem,
                  mat: Materializer): AWSCredentials =
    Await.result(AWSCredentials.default, atMost = 5 seconds).get

}
