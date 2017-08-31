package com.bluelabs

package object s3stream {
  type UploadID = String
  val MIN_CHUNK_SIZE = 5242880
}

/** todos
  * Library API
  * - one off and flow
  * - pre signed request
  *
  * API:
  * - GET bucket (list objects)
  * - list multipart
  *
  */
