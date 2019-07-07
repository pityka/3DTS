package sd

object JsonIterConfig {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  val config = CodecMakerConfig(mapMaxInsertNumber = 1024 * 1024,
                                setMaxInsertNumber = 1024 * 1024)
}
