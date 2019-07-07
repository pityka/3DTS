package sd

import intervaltree._
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import tasks.jsonitersupport._

case class GenomeCoverage(chromosome: String,
                          position: Int,
                          numberOfWellSequencedIndividuals: Int) {
  def cp = chromosome + "\t" + position
}

object GenomeCoverage {
  implicit val sk = new flatjoin.StringKey[GenomeCoverage] {
    def key(g: GenomeCoverage) = g.cp
  }
  implicit val codec: JsonValueCodec[GenomeCoverage] =
    JsonCodecMaker.make[GenomeCoverage](sd.JsonIterConfig.config)

  implicit val serde = tasks.makeSerDe[GenomeCoverage]
}

object JoinVariationsCore {

  private def addMaps[K, V](a: Map[K, V], b: Map[K, V])(
      fun: (V, V) => V): Map[K, V] = {
    a ++ b.map {
      case (key, bval) =>
        val aval = a.get(key)
        val cval = aval match {
          case None    => bval
          case Some(a) => fun((a), (bval))
        }
        (key, cval)
    }
  }

  private def mergeCons(a1: Map[Char, Consequence],
                        a2: Map[Char, Consequence]): Map[Char, Consequence] =
    addMaps(a1, a2)((_, _) match {
      case (c1, c2) if c1 == c2 => c1
      case (c1, c2) if c1 == StopGain || c2 == StopGain =>
        StopGain
      case (c1, c2) if c1 == StartLoss || c2 == StartLoss =>
        StartLoss
      case (c1, c2) if c1 == StopLoss || c2 == StopLoss =>
        StopLoss

      case (c1, c2) if c1 == NonSynonymous || c2 == NonSynonymous =>
        NonSynonymous
      case _ => Synonymous
    })

  sealed trait Bean {
    def chrpos: String
  }
  case class GnomadGenders(female: GnomadPop, male: GnomadPop)
  case class GnomadPop(total_count: Int, total_calls: Int) {
    def totalVariantAlleleCount = total_count
    def totalChromosomeCount = total_calls
  }

  case class GnomadLine(chromosome: String,
                        position: Int,
                        ref: String,
                        alt: String,
                        filter: String,
                        genders: GnomadGenders) {
    def cp = chromosome + "\t" + position
    def pass = filter == "PASS"
    def chrpos =
      chromosome + "\t" + (position - 1).toString + "\t" + position
  }

  object GnomadLine {
    implicit val sk = new flatjoin.StringKey[GnomadLine] {
      def key(g: GnomadLine) = g.cp
    }
    implicit val codec: JsonValueCodec[GnomadLine] =
      JsonCodecMaker.make[GnomadLine](sd.JsonIterConfig.config)

    implicit val serde = tasks.makeSerDe[GnomadLine]
  }

  object Bean {

    implicit val ordering: Ordering[Bean] = Ordering.by(_.chrpos)

    implicit val codec: JsonValueCodec[Bean] =
      JsonCodecMaker.make[Bean](sd.JsonIterConfig.config)

    case class Data(gl: GnomadLine, source: String, chrpos: String) extends Bean

    case class MappedCP(chrpos: String, consequence: Map[Char, Consequence])
        extends Bean

    case class Coverage(cov: GenomeCoverage, source: String, chrpos: String)
        extends Bean
  }
  import Bean._

  def readGnomad(s: scala.io.Source): Iterator[GnomadLine] =
    s.getLines
      .map { line =>
        val t = scala.util.Try(readFromString[GnomadLine](line))
        t.toOption
      }
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.pass)

  def readMappedFile(
      i: Iterator[JoinGencodeToUniprot.MapResult]): Iterator[MappedCP] =
    i.flatMap { i =>
      i match {
        case JoinGencodeToUniprot.Success(list) =>
          list.iterator.map((t1: MappedTranscriptToUniprot) =>
            MappedCP(t1.cp.s, t1.missenseConsequences.map))
        case _ => Iterator.empty
      }
    }

  def exomeIntervalTree(
      s: scala.io.Source): Map[String, IntervalTree[GFFEntry]] =
    IOHelpers
      .readGTF(s)
      .filter(_.featureType == "exon")
      .toVector
      .groupBy(_.chr)
      .map(x => x._1 -> IntervalTree.makeTree(x._2.sortBy(_.from).toList))

  def lookup(chr: String,
             bp1: Int,
             trees: Map[String, IntervalTree[GFFEntry]]): List[GFFEntry] =
    trees.get(chr).toList.flatMap { tree =>
      IntervalTree.lookup(new Interval {
        def from = bp1 - 1
        def to = bp1
      }, tree)
    }

  def join(mapped: Iterator[MappedCP],
           data: Seq[(String, Iterator[GnomadLine])],
           cov: Seq[(String, Iterator[GenomeCoverage])],
           batchSize: Int)(cpraFilter: (String, Int) => Boolean)
    : (Iterator[LocusVariationCountAndNumNs], java.io.Closeable) = {
    val sources = data.map(_._1)
    val databeans: Iterator[Data] = data.iterator.flatMap {
      case (source, it) =>
        it.filter(
            x =>
              x.ref.size == 1 && x.alt.size == 1 && x.filter == "PASS" && cpraFilter(
                x.chromosome,
                x.position))
          .map(gl => Data(gl, source, gl.chrpos))
    }
    val covbeans: Iterator[Coverage] = cov.iterator.flatMap {
      case (source, it) =>
        it.filter(x => cpraFilter(x.chromosome, x.position))
          .map(
            gl =>
              Coverage(
                gl,
                source,
                gl.chromosome + "\t" + (gl.position - 1) + "\t" + gl.position))
    }
    val (sorted: Iterator[Bean], closeable) =
      iterator.sortAsJson[Bean](databeans ++ mapped ++ covbeans, batchSize)
    val grouped: Iterator[Seq[Bean]] =
      iterator.spansByProjectionEquality(sorted)(_.chrpos)
    val merged = grouped
      .map { group =>
        val mappedTyped = group.collect {
          case x: MappedCP => x
        }
        if (mappedTyped.size == 0) None
        else {

          val dataTyped = group.collect { case x: Data         => x }
          val coverageTyped = group.collect { case x: Coverage => x }

          val chrpos = mappedTyped.head.chrpos

          val consequence: Map[Char, Consequence] =
            mappedTyped.map(_.consequence).reduce(mergeCons(_, _))

          val coverageBySource: Map[String, GenomeCoverage] =
            coverageTyped.map(x => x.source -> x.cov).toMap

          val calls = dataTyped
            .map {
              case Data(gl, source, _) =>
                val alt: Char = gl.alt.head
                val totalVariantAlleleCount = gl.genders.male.totalVariantAlleleCount + gl.genders.female.totalVariantAlleleCount
                val totalChromosomeCount = gl.genders.male.totalChromosomeCount + gl.genders.female
                  .totalChromosomeCount

                (alt, totalVariantAlleleCount, totalChromosomeCount, source)
            }
            .groupBy(_._4)
            .map {
              case (source, alts) =>
                val totalChromosomeCount = alts.map(_._3).max
                val alleleCounts: Seq[(Char, Int)] = alts.map(x => (x._1, x._2))

                (source, totalChromosomeCount, alleleCounts)
            }

          val totalSampleSize = sources.map { source =>
            val wellSequenced = coverageBySource
              .get(source)
              .map(_.numberOfWellSequencedIndividuals)
              .getOrElse(0)
            calls.find(_._1 == source).map(_._2 / 2).getOrElse(wellSequenced)
          }.sum

          val alleleCallsByConsequence: Map[Consequence, Int] = calls
            .flatMap(_._3.map(x => (x._1, x._2, consequence(x._1))))
            .groupBy(_._3)
            .map(x => x._1 -> x._2.map(_._2).sum)

          val numNs = consequence.count(_._2 == NonSynonymous)
          val numS = consequence.count(_._2 == Synonymous)

          val synAc =
            alleleCallsByConsequence.get(Synonymous).getOrElse(0)
          val nonsynAc =
            alleleCallsByConsequence.get(NonSynonymous).getOrElse(0)

          Some(
            LocusVariationCountAndNumNs(locus = ChrPos(chrpos),
                                        numNs = numNs,
                                        alleleCountSyn = synAc,
                                        alleleCountNonSyn = nonsynAc,
                                        sampleSize = totalSampleSize,
                                        numS = numS))

        }
      }
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.sampleSize > 0)

    (merged, closeable)
  }

}
