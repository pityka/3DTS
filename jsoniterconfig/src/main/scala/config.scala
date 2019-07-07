package sd

object JsonIterConfig {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  val config = CodecMakerConfig(mapMaxInsertNumber = Int.MaxValue,
                                setMaxInsertNumber = Int.MaxValue)
}
