
/* 
* The MIT License
*
* Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland, 
* Group Fellay
*
* Permission is hereby granted, free of charge, to any person obtaining
* a copy of this software and associated documentation files (the "Software"),
* to deal in the Software without restriction, including without limitation 
* the rights to use, copy, modify, merge, publish, distribute, sublicense, 
* and/or sell copies of the Software, and to permit persons to whom the Software
* is furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
* SOFTWARE.
*/

package index2

//Produces an Iterator[Seq[A]] from an Iterator[A] by the continuous spans of a projection.
final class SpanIterator[A, T](iter: Iterator[A], p: A => T) extends Iterator[Vector[A]] {

  val buffer = collection.mutable.ArrayBuffer[A]()

  var currentPredicateValue: T = _

  var fwRead: Option[A] = if (iter.hasNext) Some(iter.next) else None

  fwRead.foreach { x =>
    currentPredicateValue = p(x)
  }

  def fw = if (iter.hasNext) {
    fwRead = Some(iter.next)
  } else {
    fwRead = None
  }

  def hasNext = fwRead.isDefined

  def next = {

    while (fwRead.isDefined && p(fwRead.get) == currentPredicateValue) {

      buffer.append(fwRead.get)

      fw
    }

    val ret = buffer.toVector

    buffer.clear

    fwRead.foreach { x =>
      currentPredicateValue = p(x)
    }

    ret

  }

}
