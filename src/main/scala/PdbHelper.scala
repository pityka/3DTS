package sd

import org.saddle._

object PdbHelper {

  def parsePdbLines(lines: Iterator[String]): CIFContents = {
    val atoms = lines
      .filter(_.startsWith("ATOM  "))
      .map { atomLine =>
        val serial = atomLine.substring(6, 11).toInt
        val atomName = atomLine.substring(12, 16).trim
        val residueName = atomLine.substring(17, 20).trim
        val chain = atomLine(21).toString
        val residueNumber = atomLine.substring(22, 26).toInt
        val insertionCode = atomLine(26) match {
          case ' ' => None
          case x   => Some(x.toString)
        }
        val coordX = atomLine.substring(30, 38).toDouble
        val coordY = atomLine.substring(38, 46).toDouble
        val coordZ = atomLine.substring(46, 54).toDouble

        val tempFactor = atomLine.substring(60, 66).trim

        val elementSymbol = atomLine.substring(76, 78).trim
        val atom = Atom(coord = Vec(coordX, coordY, coordZ),
                        id = serial,
                        name = atomName,
                        symbol = elementSymbol,
                        residue3 = residueName,
                        tempFactor = tempFactor)

        (atom,
         PdbChain(chain),
         PdbResidueNumber(residueNumber, insertionCode),
         FakePdbChain(chain))

      }
      .toVector
    val chainremap = atoms.map(r => r._4 -> r._2).distinct.toMap
    CIFContents(Map(), Structure(atoms, chainremap))
  }
}
