import htsjdk.samtools.reference._
import stringsplit._
import java.io.File

object HeptamerHelpers {

  def heptamerAt(cp: ChrPos, genome: IndexedFastaSequenceFile): String = {
    val Seq(chr, bp0, _) = cp.s.split1('\t')
    genome
      .getSubsequenceAt(chr, bp0.toLong - 2, bp0.toLong + 3)
      .getBaseString()
      .toUpperCase
  }

  def heptamerAt(chromosome: String,
                 position: Int,
                 genome: IndexedFastaSequenceFile): String = {
    genome
      .getSubsequenceAt(chromosome, position.toLong - 2, position.toLong + 3)
      .getBaseString()
      .toUpperCase
  }

  def openFasta(fasta: File, fai: File): IndexedFastaSequenceFile = {
    val idx = new FastaSequenceIndex(fai)
    new IndexedFastaSequenceFile(fasta, idx)
  }

  def readFrequencies(source: scala.io.Source): Map[String, Double] =
    source.getLines.map { i =>
      val spl = i.splitM(Set(' ', '\t'))
      spl(0) -> spl(1).toDouble
    }.toMap

}
