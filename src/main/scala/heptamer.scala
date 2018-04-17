import htsjdk.samtools.reference._
import stringsplit._
import java.io.File
import scala.util.Try

object HeptamerHelpers {

  def heptamerAt(cp: ChrPos, genome: IndexedFastaSequenceFile): Try[String] =
    Try {
      val Seq(chr, bp0, _) = cp.s.split1('\t')
      genome
        .getSubsequenceAt(chr.drop(3), bp0.toLong - 2, bp0.toLong + 4)
        .getBaseString()
        .toUpperCase
    }

  def heptamerAt(chromosome: String,
                 bp1: Int,
                 genome: IndexedFastaSequenceFile): Try[String] = Try {
    genome
      .getSubsequenceAt(chromosome.drop(3), bp1.toLong - 3, bp1.toLong + 3)
      .getBaseString()
      .toUpperCase
  }

  def openFasta(fasta: File, fai: File): IndexedFastaSequenceFile = {
    val idx = new FastaSequenceIndex(fai)
    new IndexedFastaSequenceFile(fasta, idx)
  }

  def readRates(source: scala.io.Source): Map[String, Double] =
    source.getLines.map { i =>
      val spl = i.splitM(Set(' ', '\t'))
      spl(0) -> spl(1).toDouble
    }.toMap

}
