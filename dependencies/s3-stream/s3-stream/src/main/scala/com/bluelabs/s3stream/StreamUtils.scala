package com.bluelabs.s3stream

import akka.NotUsed
import akka.stream.scaladsl.Source
import scala.concurrent.Future

object StreamUtils {
  def counter(initial: Int = 0): Source[Int, NotUsed] = {
    Source.unfold(initial)((i: Int) => Some(i + 1, i))
  }

  /** Copyright notice for singleLazyAsync and singleLazy
    *
    * These two methods are copied from https://github.com/MfgLabs/akka-stream-extensions/blob/master/commons/src/main/scala/SourceExt.scala
    */
  /**
    * Create a source from a Lazy Async value that will be evaluated only when the stream is materialized.
    *
    * @param fut
    * @tparam A
    * @return
    */
  def singleLazyAsync[A](fut: => Future[A]): Source[A, NotUsed] =
    singleLazy(fut).mapAsync(1)(identity)

  /**
    * Create a source from a Lazy Value that will be evaluated only when the stream is materialized.
    *
    * @param a
    * @tparam A
    * @return
    */
  def singleLazy[A](a: => A): Source[A, NotUsed] =
    Source.single(() => a).map(_())
}
