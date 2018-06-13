package sd
object CompareMethods {
  case class Datum(pdb: String,
                   feature: String,
                   score: String,
                   r2: String,
                   value: Double)

  def analyzeProjection[T](data: List[Datum])(k1: Datum => T,
                                              k2: Datum => String) =
    data
      .groupBy(k1)
      .toList
      .flatMap {
        case (_, values) =>
          val cell = values
            .map(d => (k2(d), d.value))
            .sortBy(_._2)
            .map(_._1)
            .reverse
            .zipWithIndex;
          cell.map(c => (c._1 -> c._2.toDouble / cell.size))
      }
      .groupBy(_._1)
      .toList
      .map {
        case (key, ranks) => (key, ranks.map(_._2).sum / ranks.size.toDouble)
      }
      .sortBy(_._2)
}

// scala> println("by feature: \n" +analyzeProjection(parsedData)((d:Datum) => (d.pdb,d.score,d.r2), (d:Datum) => d.feature).mkString("\n"))
// by feature:
// (SSE only,0.28368556701030917)
// (All Features,0.33280927835051555)

// scala> println("by score: \n" +analyzeProjection(parsedData)((d:Datum) => (d.pdb,d.feature,d.r2), (d:Datum) => d.score).mkString("\n"))
// by score:
// (nsPostHeptamerIndependentChromosomeSpecificIntergenicRate,0.21025773195876146)
// (nsPostHeptamerIndependentIntergenicRate,0.3332044673539536)
// (nsPostHeptamerSpecificIntergenicRate,0.4577061855670135)
// (nsPostGlobalSynonymousRate (Old 3DTS),0.5304467353951846)
// (nsPostHeptamerSpecificChromosomeSpecificIntergenicRate,0.5848797250859018)

// scala> println("by r2: \n" +analyzeProjection(parsedData)((d:Datum) => (d.pdb,d.feature,d.score), (d:Datum) => d.r2).mkString("\n"))
// by r2:
// (Pearson,0.26926116838487973)
// (Spearman,0.347233676975945)