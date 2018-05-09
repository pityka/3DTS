package sd

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

/**
  * Representation of the genetic code.
  *
  * ref: http://en.wikipedia.org/wiki/Genetic_code
  */
object GeneticCode {

  /**
    * Translates nucleotide sequence to amino acid sequence.
    *
    * @param nucleotides: string of nucleotides. can be uncapitalized. gaps and unknown are translated to X
    * @return amino acid sequence
    */
  def translate(nucleotides: String, keepGapsInFrame: Boolean): String = {
    val withOrWithOutGaps =
      if (keepGapsInFrame) nucleotides.replaceAllLiterally("-", "N")
      else nucleotides.replaceAllLiterally("-", "")
    (withOrWithOutGaps.toUpperCase grouped (3) map GeneticCode.codon2aa).mkString
  }

  private val codonregexp = "^[ATGCN]{1,3}$".r

  private val ambiguousNucleotides = Map(
    'A' -> ('A' :: Nil),
    'T' -> ('T' :: Nil),
    'G' -> ('G' :: Nil),
    'C' -> ('C' :: Nil),
    'R' -> ('A' :: 'G' :: Nil),
    'Y' -> ('C' :: 'T' :: Nil),
    'S' -> ('G' :: 'C' :: Nil),
    'W' -> ('A' :: 'T' :: Nil),
    'K' -> ('G' :: 'T' :: Nil),
    'M' -> ('A' :: 'C' :: Nil),
    'B' -> ('C' :: 'G' :: 'T' :: Nil),
    'D' -> ('A' :: 'G' :: 'T' :: Nil),
    'H' -> ('A' :: 'C' :: 'T' :: Nil),
    'V' -> ('A' :: 'C' :: 'G' :: Nil),
    'N' -> ('A' :: 'C' :: 'G' :: 'T' :: Nil)
  ).withDefaultValue(List('X'))

  /** From http://en.wikipedia.org/wiki/Genetic_code */
  private val codontable = Map(
    "TTT" -> 'F',
    "TTC" -> 'F',
    "TTA" -> 'L',
    "TTG" -> 'L',
    "CTT" -> 'L',
    "CTC" -> 'L',
    "CTA" -> 'L',
    "CTG" -> 'L',
    "ATT" -> 'I',
    "ATC" -> 'I',
    "ATA" -> 'I',
    "ATG" -> 'M',
    "GTT" -> 'V',
    "GTC" -> 'V',
    "GTA" -> 'V',
    "GTG" -> 'V',
    "TCT" -> 'S',
    "TCC" -> 'S',
    "TCA" -> 'S',
    "TCG" -> 'S',
    "CCT" -> 'P',
    "CCC" -> 'P',
    "CCA" -> 'P',
    "CCG" -> 'P',
    "ACT" -> 'T',
    "ACC" -> 'T',
    "ACA" -> 'T',
    "ACG" -> 'T',
    "GCT" -> 'A',
    "GCC" -> 'A',
    "GCA" -> 'A',
    "GCG" -> 'A',
    "TAT" -> 'Y',
    "TAC" -> 'Y',
    "TAA" -> '*',
    "TAG" -> '*',
    "CAT" -> 'H',
    "CAC" -> 'H',
    "CAA" -> 'Q',
    "CAG" -> 'Q',
    "AAT" -> 'N',
    "AAC" -> 'N',
    "AAA" -> 'K',
    "AAG" -> 'K',
    "GAT" -> 'D',
    "GAC" -> 'D',
    "GAA" -> 'E',
    "GAG" -> 'E',
    "TGT" -> 'C',
    "TGC" -> 'C',
    "TGA" -> '*',
    "TGG" -> 'W',
    "CGT" -> 'R',
    "CGC" -> 'R',
    "CGA" -> 'R',
    "CGG" -> 'R',
    "AGT" -> 'S',
    "AGC" -> 'S',
    "AGA" -> 'R',
    "AGG" -> 'R',
    "GGT" -> 'G',
    "GGC" -> 'G',
    "GGA" -> 'G',
    "GGG" -> 'G'
  )

  def hasAmbiguousNucleotideCodes(s: String) =
    s.filter(x => x != 'A' && x != 'T' && x != 'C' && x != 'G').size > 0

  /**
    * Translates codons to amino acids.
    *
    * @note codon must be capitalized.
    * @param s codon to translate. Must be capitalized.
    * @return capitalized amino acid code (IUPAC).
    */
  def codon2aa(s: String): Char = {
    require(s.size <= 3 && s.size > 0 && !s.contains('-'))
    if (s.size < 3) 'X'
    else {
      // Every element of that list is a letter of a codon (with multiple possibilities)
      val ambiguousResolved: Seq[List[Char]] = s map ambiguousNucleotides

      // Every element of this list is a codon
      val crossed: Seq[String] = for {
        a <- ambiguousResolved(0);
        b <- ambiguousResolved(1);
        c <- ambiguousResolved(2)
      } yield List(a, b, c).mkString

      val aa: Seq[Char] =
        (crossed map (x => codontable.get(x).getOrElse('X'))).distinct

      if (aa.size == 1) aa.head else 'X'
    }
  }

  /**
    * Translates codons to amino acids.
    *
    * @note codon must be capitalized.
    * @param s codon to translate. Must be capitalized.
    * @return capitalized amino acid code (IUPAC).
    */
  def codon2possibleAas(s: String): Seq[Char] = {
    require(s.size <= 3 && s.size > 0 && !s.contains('-'))
    if (s.size < 3) List('X')
    else {
      // Every element of that list is a letter of a codon (with multiple possibilities)
      val ambiguousResolved: Seq[List[Char]] = s map ambiguousNucleotides

      // Every element of this list is a codon
      val crossed: Seq[String] = for {
        a <- ambiguousResolved(0);
        b <- ambiguousResolved(1);
        c <- ambiguousResolved(2)
      } yield List(a, b, c).mkString

      val aa: Seq[Char] =
        (crossed map (x => codontable.get(x).getOrElse('X'))).distinct

      aa.distinct
    }
  }

  /**
    * Translates amino acid codes to codons
    *
    * Multiple codons can match a single amino acid.
    * Amino acid code "X" matches all codons with undefined nucleotides ("N")
    *
    * @param s amino acid code
    * @return List of matching codons
    */
  // def aa2codons(s: String) = {
  //   if (s == "X") leakyCodons
  //   else codontable.groupBy(_._2).mapValues(x => x.map(_._1)).apply(s)
  // }

  /** Decides on aa-codon match. */
  def matches(codon: String, aa: Char): Boolean =
    (codon2possibleAas(codon).contains(aa) || (aa == 'X' && hasAmbiguousNucleotideCodes(
      codon)))

}
