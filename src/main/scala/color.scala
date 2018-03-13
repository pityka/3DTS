trait Colormap { self =>
  def apply(v: Double): Color
  def withRange(min: Double, max: Double): Colormap
}

case class Color(r: Int, g: Int, b: Int, a: Int) extends Colormap {
  def apply(v: Double) = this
  def withRange(min: Double, max: Double) = this
}

case class RedBlue(min: Double = 0.0, max: Double = 1.0, mid: Double = 0.5)
    extends Colormap {

  def apply(value: Double): Color = {

    val (r, g, b) = if (value > mid) {
      val v =
        if (value > max) 1.0
        else if (value < min) 0.0
        else (value - mid) / (max - mid)
      val v2 = 1 - v
      (1d, v2, v2)
    } else {
      val v =
        if (value < min) 1.0
        else if (value > max) 0.0
        else (mid - value) / (mid - min)
      val v2 = 1 - v
      (v2, v2, 1d)
    }

    Color((r * 255).toInt, (g * 255).toInt, (b * 255).toInt, 255)
  }

  def withRange(min: Double, max: Double) = RedBlue(min, max, mid)
}
