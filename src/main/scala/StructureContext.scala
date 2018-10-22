package sd

import com.thesamet.spatial._
import org.saddle._
import com.typesafe.scalalogging.StrictLogging

object StructureContext extends StrictLogging {

  def featureFromSlidingWindow(
      cif: CIFContents,
      windowSize: Int,
      radius: Double): List[((PdbChain, PdbResidueNumber),
                             Seq[(PdbChain, PdbResidueNumber)])] = {
    assert(windowSize % 2 == 1, "even windowSize")
    val atomsByResidue = cif.assembly.atoms
      .groupBy(x => (x.pdbChain, x.pdbResidueNumber))
      .map(x => x._1 -> x._2.map(_.atom))

    type T1 = AtomWithLabels

    implicit val dimensionalOrderingForAtom: DimensionalOrdering[T1] =
      new DimensionalOrdering[T1] {
        val dimensions = 3

        def compareProjection(d: Int)(x: T1, y: T1) =
          x.atom.coord.raw(d).compareTo(y.atom.coord.raw(d))
      }

    def liftIntoFakeT1(v: Vec[Double]) =
      AtomWithLabels(Atom(v, -1, "", "", "", ""),
                     PdbChain("?"),
                     PdbResidueNumber(-1, None),
                     FakePdbChain("?"))

    val kdtree: KDTree[T1] = KDTree(cif.assembly.atoms: _*)

    val jump = windowSize / 2

    cif.aminoAcidSequence.flatMap {
      case (chain, aa) =>
        val sorted = aa.sortBy(_._2).toVector
        val length = sorted.length
        val halfWindow = windowSize / 2
        val start = 0
        val end = length - 1
        start to end by jump map { center =>
          val left = math.max(center - halfWindow, 0)
          val right = math.min(center + halfWindow, end)
          val centerResidueOfFeature: PdbResidueNumber = sorted(center)._2
          val featureAtoms: List[Atom] = (left to right flatMap { idx =>
            val residue = sorted(idx)._2
            atomsByResidue.get(chain -> residue).toVector.flatten
          } toList)

          val expandedAtoms2: Vector[T1] =
            featureAtoms
              .flatMap { fa =>
                val bbXTop = fa.coord.raw(0) + radius
                val bbXBot = fa.coord.raw(0) - radius
                val bbYTop = fa.coord.raw(1) + radius
                val bbYBot = fa.coord.raw(1) - radius
                val bbZTop = fa.coord.raw(2) + radius
                val bbZBot = fa.coord.raw(2) - radius
                val queryRegion = Region
                  .from(liftIntoFakeT1(Vec(bbXBot, 0d, 0d)), 0)
                  .to(liftIntoFakeT1(Vec(bbXTop, 0d, 0d)), 0)
                  .from(liftIntoFakeT1(Vec(0d, bbYBot, 0d)), 1)
                  .to(liftIntoFakeT1(Vec(0d, bbYTop, 0d)), 1)
                  .from(liftIntoFakeT1(Vec(0d, 0d, bbZBot)), 2)
                  .to(liftIntoFakeT1(Vec(0d, 0d, bbZTop)), 2)

                kdtree
                  .regionQuery(queryRegion)
                  .filter(atomWithPdb =>
                    fa.within(radius, atomWithPdb.atom.coord))
              }
              .distinct
              .toVector

          val expandedResidues =
            expandedAtoms2.map(x => (x.pdbChain, x.pdbResidueNumber)).distinct

          (chain -> centerResidueOfFeature) -> expandedResidues

        }
    }.toList

  }

  def featureFromUniprotFeatureSegmentation(
      pdbId: PdbId,
      cif: CIFContents,
      features: Seq[(PdbChain, UniprotFeatureName, Set[PdbResidueNumber])],
      radius: Double,
      bothSidesOfSpace: Boolean)
    : List[((PdbChain, UniprotFeatureName, PdbResidueNumber, PdbResidueNumber),
            Seq[(PdbChain, PdbResidueNumber)])] = {

    val atomsByResidue = cif.assembly.atoms
      .groupBy(x => (x.pdbChain, x.pdbResidueNumber))
      .map(x => x._1 -> x._2.map(_.atom))

    type T1 = AtomWithLabels

    implicit val dimensionalOrderingForAtom: DimensionalOrdering[T1] =
      new DimensionalOrdering[T1] {
        val dimensions = 3

        def compareProjection(d: Int)(x: T1, y: T1) =
          x.atom.coord.raw(d).compareTo(y.atom.coord.raw(d))
      }

    def liftIntoFakeT1(v: Vec[Double]) =
      AtomWithLabels(Atom(v, -1, "", "", "", ""),
                     PdbChain("?"),
                     PdbResidueNumber(-1, None),
                     FakePdbChain("?"))

    val kdtree: KDTree[T1] = KDTree(cif.assembly.atoms: _*)

    features
      .filter(x => x._3.size > 0 && x._3.size < 200)
      .map {
        case (chain, featureName, residuesInFeature) =>
          val featureAtoms: List[List[Atom]] = (residuesInFeature map {
            residue =>
              atomsByResidue.get(chain -> residue).toList.flatten
          } toList)

          logger.debug(
            s"$pdbId - $chain - $featureName - ${residuesInFeature.size} - ${featureAtoms.flatten.size}")

          val expandedAtoms2: Vector[T1] =
            featureAtoms
              .flatMap { atomsOfFeatureResidue =>
                atomsOfFeatureResidue.flatMap { fa =>
                  val bbXTop = fa.coord.raw(0) + radius
                  val bbXBot = fa.coord.raw(0) - radius
                  val bbYTop = fa.coord.raw(1) + radius
                  val bbYBot = fa.coord.raw(1) - radius
                  val bbZTop = fa.coord.raw(2) + radius
                  val bbZBot = fa.coord.raw(2) - radius
                  val queryRegion = Region
                    .from(liftIntoFakeT1(Vec(bbXBot, 0d, 0d)), 0)
                    .to(liftIntoFakeT1(Vec(bbXTop, 0d, 0d)), 0)
                    .from(liftIntoFakeT1(Vec(0d, bbYBot, 0d)), 1)
                    .to(liftIntoFakeT1(Vec(0d, bbYTop, 0d)), 1)
                    .from(liftIntoFakeT1(Vec(0d, 0d, bbZBot)), 2)
                    .to(liftIntoFakeT1(Vec(0d, 0d, bbZTop)), 2)

                  kdtree
                    .regionQuery(queryRegion)
                    .filter(atomWithPdb =>
                      fa.within(radius, atomWithPdb.atom.coord))
                    .filter(atomWithPdb =>
                      bothSidesOfSpace || atomAlignsWithSideChain(
                        atomWithPdb.atom,
                        atomsOfFeatureResidue))
                }
              }
              .distinct
              .toVector

          if (expandedAtoms2.isEmpty) {
            logger.info(
              s"Empty feature. $pdbId - $chain - $featureName $featureAtoms ${cif.assembly.atoms.size} $residuesInFeature $atomsByResidue")
          }

          val expandedResidues =
            expandedAtoms2.map(x => (x.pdbChain, x.pdbResidueNumber)).distinct

          (chain,
           featureName,
           residuesInFeature.toSeq.sorted.min,
           residuesInFeature.toSeq.sorted.max) -> expandedResidues

      }
      .toList

  }

  def atomAlignsWithSideChain(atom: Atom, residue: List[Atom]): Boolean = {
    val alphaCarbon = residue.find(_.name == "CA").map(_.coord)
    val betaCarbon = residue.find(_.name == "CB").map(_.coord)
    if (alphaCarbon.isDefined && betaCarbon.isDefined) {
      val normal = alphaCarbon.get - betaCarbon.get
      val point = alphaCarbon.get
      val atomCoordinate = atom.coord
      val atomVector = point - atomCoordinate
      val dot = atomVector dot normal
      dot > 0
    } else false // do not expand glycines
  }

}
