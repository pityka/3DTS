package sd

import stringsplit._
import IOHelpers.three2One
import org.saddle._
import org.saddle.linalg._
import scala.util.Try
case class PdbAsssemblyId(i: String) extends AnyVal

case class FakePdbChain(s: String) extends AnyVal

case class Atom(coord: Vec[Double],
                id: Int,
                name: String,
                symbol: String,
                residue3: String,
                tempFactor: String) {
  def euclid(point: Vec[Double]) = {
    var s = 0d
    var i = 0
    while (i < point.length) {
      val d = (point.raw(i) - coord.raw(i))
      s += d * d
      i += 1
    }
    val r = math.sqrt(s)

    r
  }
  def within(dist: Double, point: Vec[Double]) = euclid(point) < dist
}

/* Not augmented */
case class AffineTransformation(m: Mat[Double], b: Vec[Double]) {
  assert(m.numRows == 3)
  assert(m.numCols == 3)
  assert(b.length == 3)

  def apply(v: Vec[Double]) = (m mv v) + b

  def andThen(t: AffineTransformation) = {
    val m3 = t.m mm this.m
    val b3 = (t.m mv b) + t.b
    AffineTransformation(m3, b3)
  }

}

object AffineTransformation {
  val identity =
    AffineTransformation(org.saddle.mat.ident(3), org.saddle.vec.zeros(3))
}

case class Structure(
    atoms: Vector[(Atom, PdbChain, PdbResidueNumber, FakePdbChain)],
    pdbChainRemap: Map[FakePdbChain, PdbChain],
    helix: Seq[PdbHelix] = Nil,
    sheet: Seq[PdbSheet] = Nil)

case class PdbHelix(serial: String,
                    id: String,
                    initialResidueName: String,
                    initChain: String,
                    initResSeqNum: Int,
                    initResInsertion: String,
                    terminalResidueName: String,
                    terminalChain: String,
                    terminalResSeqNum: Int,
                    terminalResInsertion: String,
                    helixType: Option[Int],
                    comment: String,
                    helixLength: Option[Int])

case class PdbSheet(strand: Int,
                    id: String,
                    numberOfStrands: Int,
                    initResName: String,
                    initChain: String,
                    initNum: Int,
                    initCode: String,
                    terminalResName: String,
                    terminalChain: String,
                    terminalNum: Int,
                    terminalCode: String,
                    sense: Int,
                    atomName1: Option[String],
                    resName1: Option[String],
                    chain1: Option[String],
                    num1: Option[Int],
                    code1: Option[String],
                    atomName2: Option[String],
                    resName2: Option[String],
                    chain2: Option[String],
                    num2: Option[Int],
                    code2: Option[String])

case class CIFContents(
    aminoAcidSequence: Map[PdbChain, List[(Char, PdbResidueNumber)]],
    assembly: Structure) {

  def asPDB: String = {
    def rightJustify(s: String, w: Int) = (" " * (w - s.size)) + s
    def leftJustify(s: String, w: Int) = s + (" " * (w - s.size))

    val fakePdbChainsReverse =
      assembly.pdbChainRemap.toSeq
        .map(x => x._1.s -> x._2.s)
        .groupBy(_._2)
        .map(x => x._1 -> x._2.map(_._1))

    val sheetTable = assembly.sheet
      .filter(x =>
        x.initChain == x.terminalChain && fakePdbChainsReverse.contains(
          x.initChain))
      .flatMap { pdb =>
        val ch = pdb.initChain
        val fakeChains = fakePdbChainsReverse(ch)
        fakeChains.map { ch =>
          pdb.copy(initChain = ch,
                   terminalChain = ch,
                   chain1 = pdb.chain1.map(_ => ch),
                   chain2 = pdb.chain2.map(_ => ch))
        }
      }
      .map {
        case PdbSheet(strand: Int,
                      id: String,
                      numberOfStrands: Int,
                      initResName: String,
                      initChain: String,
                      initNum: Int,
                      initCode: String,
                      terminalResName: String,
                      terminalChain: String,
                      terminalNum: Int,
                      terminalCode: String,
                      sense: Int,
                      atomName1: Option[String],
                      resName1: Option[String],
                      chain1: Option[String],
                      num1: Option[Int],
                      code1: Option[String],
                      atomName2: Option[String],
                      resName2: Option[String],
                      chain2: Option[String],
                      num2: Option[Int],
                      code2: Option[String]) =>
          List(
            "SHEET  ",
            rightJustify(strand.toString, 3),
            " ",
            rightJustify(id, 3),
            rightJustify(numberOfStrands.toString, 2),
            " ",
            rightJustify(initResName, 2),
            " ",
            rightJustify(initChain, 1),
            rightJustify(initNum.toString, 4),
            rightJustify(initCode, 1),
            " ",
            rightJustify(terminalResName, 2),
            " ",
            rightJustify(terminalChain, 1),
            rightJustify(terminalNum.toString, 4),
            rightJustify(terminalCode, 1),
            rightJustify(sense.toString, 2),
            " ",
            leftJustify(atomName1.getOrElse(""), 4),
            rightJustify(resName1.getOrElse(""), 3),
            " ",
            rightJustify(chain1.getOrElse(""), 1),
            rightJustify(num1.getOrElse("").toString, 4),
            rightJustify(code1.getOrElse(""), 1),
            " ",
            leftJustify(atomName2.getOrElse(""), 4),
            rightJustify(resName2.getOrElse(""), 3),
            " ",
            rightJustify(chain2.getOrElse(""), 1),
            rightJustify(num2.getOrElse("").toString, 4),
            rightJustify(code2.getOrElse(""), 1)
          ).mkString
      }
      .mkString("\n")

    val atomTable = assembly.atoms
      .map {
        case (Atom(coords, atomId, name, symbol, residueName, temp),
              _,
              PdbResidueNumber(residue, maybeInsertionCode),
              FakePdbChain(chain)) =>
          List(
            "ATOM  ",
            rightJustify(atomId.toString, 5),
            " ",
            leftJustify(name, 4),
            " ",
            rightJustify(residueName.take(3), 3),
            " " + rightJustify(chain, 1),
            rightJustify(residue.toString, 4),
            maybeInsertionCode.getOrElse(" "),
            "   ",
            rightJustify(coords.raw(0).toString.take(7), 8),
            rightJustify(coords.raw(1).toString.take(7), 8),
            rightJustify(coords.raw(2).toString.take(7), 8),
            rightJustify("1.0", 6),
            rightJustify(temp, 6),
            "      ",
            rightJustify("", 4),
            rightJustify(symbol, 2),
            " "
          ).mkString
      }
      .mkString("\n")
    val helixTable = assembly.helix
      .filter(x =>
        x.initChain == x.terminalChain && fakePdbChainsReverse.contains(
          x.initChain))
      .flatMap { pdb =>
        val ch = pdb.initChain
        val fakeChains = fakePdbChainsReverse(ch)
        fakeChains.map { ch =>
          pdb.copy(initChain = ch, terminalChain = ch)
        }
      }
      .map {
        case PdbHelix(serial,
                      id,
                      initialResidueName,
                      initChain,
                      initResSeqNum,
                      initResInsertion,
                      terminalResidueName,
                      terminalChain,
                      terminalResSeqNum,
                      terminalResInsertion,
                      helixType,
                      comment,
                      helixLength) =>
          List(
            "HELIX  ",
            rightJustify(serial.toString, 3),
            " ",
            rightJustify(id, 3),
            " ",
            rightJustify(initialResidueName, 3),
            " ",
            rightJustify(initChain, 1),
            " ",
            rightJustify(initResSeqNum.toString, 4),
            rightJustify(initResInsertion, 1),
            " ",
            rightJustify(terminalResidueName, 3),
            " ",
            rightJustify(terminalChain, 1),
            " ",
            rightJustify(terminalResSeqNum.toString, 4),
            rightJustify(terminalResInsertion, 1),
            rightJustify(helixType.getOrElse("").toString, 2),
            rightJustify(comment, 30),
            " ",
            rightJustify(helixLength.getOrElse("").toString, 5)
          ).mkString
      }
      .mkString("\n")

    val remapRemark =
      assembly.pdbChainRemap.map(x => x._1.s + ":" + x._2.s).mkString(":")

    s"REMARK 9 $remapRemark\n" + helixTable + "\n" + sheetTable + "\n" + atomTable
  }

}

object CIFContents {

  /** Extract necessary information from the protein structure file
    *
    * Reads the Atom table of the CIF file.
    * Extracts: Residue, PDB Residue Number, PDB Insertion Code, PDB Chain
    *
    * From: http://mmcif.wwpdb.org/docs/tutorials/content/atomic-description.html
    *
    * _atom_site.auth_asym_id This item holds the PDB chain identifier
    * _atom_site.auth_comp_id This item holds the PDB 3-letter-code residue names
    * _atom_site.auth_seq_id This item holds the PDB residue number
    * _atom_site.pdbx_PDB_ins_code This data item corresponds to the PDB insertion code
    * _atom_site.label_seq_id: This data item is a pointer to _entity_poly_seq.num in the
    *           ENTITY_POLY_SEQ category.
    *
    * _struct_ref_seq_dif: Data items in the STRUCT_REF_SEQ_DIF category provide a
    *           mechanism for indicating and annotating point differences
    *           between the sequence of the entity or biological unit described
    *           in the data block and the sequence of the referenced database
    *           entry.
    *  _struct_ref_seq_dif.seq_num: This data item is a pointer to _entity_poly_seq.num in the
    *           ENTITY_POLY_SEQ category.
    * _struct_ref_seq_dif.pdbx_pdb_strand_id: PDB strand/chain id.
    * _struct_ref_seq_dif.pdbx_pdb_ins_code : PDB Insertion code
    * _struct_ref_seq_dif.pdbx_auth_seq_num:
    * _struct_ref_seq_dif.pdbx_seq_db_name: external database name, we need UNP
    * _struct_ref_seq_dif.db_mon_id :  The monomer type found at this position in the referenced
    *           database entry.
    *
    * Residues are read primarily form the _atom_site table (_atom_site.auth_comp_id ).
    * If the _atom_site.auth_comp_id + _atom_site.pdbx_PDB_ins_code in present
    * in _struct_ref_seq_dif (pdbx_auth_seq_num, pdbx_pdb_ins_code), then
    * _struct_ref_seq_dif.db_mon_id is used instead of _atom_site.auth_comp_id
    *
    * Non polyprotein chains are excluded, i.e. if _entity_poly.type does not contain 'polyprotein'
    *
    * The 3 letter codes are always looked up in `three2One` (see below) and if not found then X is used.
    *
    * Tuples are returned sorted by the PdbResidueNumber, see the sorting defined
    * in the companion object of that class
    *
    * Biological Assembly:
    * Tables needed:
    * _pdbx_struct_oper_list
    * _pdbx_struct_assembly
    * _pdbx_struct_assembly_gen
    * _atom_site
    * Follows: http://mmcif.wwpdb.org/docs/sw-examples/cpp/html/assemblies.html
    *
    * For each assembly in _pdbx_struct_assembly_gen:
    *  Compute the affine transformation matrix from _pdbx_struct_oper_list using the
    *  _pdbx_struct_assembly_gen.oper_expression field
    * Map each atom with each operation (creating as many images as many operations) in the prescribed chains
    */
  def parseCIF(allLines: List[String]): Try[CIFContents] = Try {

    case class CifChain(s: String)

    def parseTable(
        category: String,
        fields: IndexedSeq[String]): (Seq[IndexedSeq[Option[String]]]) = {
      val loop = allLines
        .sliding(2)
        .find(x => x(0).trim == "loop_" && x(1).trim.startsWith(category + "."))
        .isDefined
      val multiLineRegexp =
        "([^\';\\s\n\r\"][^\\s\n\r]*|\'.+?\'|(?s);.+?;|\".+?\")".r
      if (loop) {
        val tableLines = allLines
          .dropWhile(x => !x.startsWith(category + "."))
          .takeWhile(x => !x.startsWith("#"))
        val (header1, table) =
          tableLines.span(_.startsWith(category))
        val header = header1.map(_.trim)
        val idx = fields.map { fi =>
          fi -> header.indexOf(category + "." + fi)
        }
        val mIdx = idx.map(_._2).max

        multiLineRegexp
          .findAllMatchIn(table.mkString("\n"))
          .map(_.matched)
          .grouped(header.size)
          .filter(_.size >= mIdx)
          .map { spl =>
            idx.map {
              case (_, fieldIdx) =>
                if (fieldIdx >= 0) Some(spl(fieldIdx))
                else None
            }
          }
          .toList
      } else {
        val kvlines: Map[String, String] = {
          val wholeFile = allLines.mkString("\n")
          val regex =
            (category + "\\.([^\\.\\s]+)\\s*(?:\'(.+?)\'|(?s);(.+?);|\"(.+?)\"|([^\\s]*))").r

          regex
            .findAllMatchIn(wholeFile)
            .map(m => m.subgroups.filter(_ != null))
            .toList
            .filter(_.size == 2)
            .map(x => x(0) -> x(1).split("\\n").mkString)
            .toMap

        }

        if (kvlines.isEmpty) Nil
        else
          List(fields.map { field =>
            kvlines.get(field)
          })
      }

    }

    val helix: Seq[PdbHelix] =
      parseTable(
        "_struct_conf",
        Vector(
          "id",
          "pdbx_PDB_helix_id",
          "beg_auth_comp_id",
          "beg_auth_asym_id",
          "beg_auth_seq_id",
          "pdbx_beg_PDB_ins_code",
          "end_auth_comp_id",
          "end_auth_asym_id",
          "end_auth_seq_id",
          "pdbx_end_PDB_ins_code",
          "pdbx_PDB_helix_class",
          "details",
          "pdbx_PDB_helix_length"
        )
      ).map { spl =>
        PdbHelix(
          serial = spl(1).get,
          id = spl(1).get,
          initialResidueName = spl(2).get,
          initChain = spl(3).get,
          initResSeqNum = spl(4).get.toInt,
          initResInsertion = spl(5).filter(_ != "?").getOrElse(""),
          terminalResidueName = spl(6).get,
          terminalChain = spl(7).get,
          terminalResSeqNum = spl(8).get.toInt,
          terminalResInsertion = spl(9).filter(_ != "?").getOrElse(""),
          helixType = spl(10).filter(_ != "?").map(_.toInt),
          comment = spl(11).filter(_ != "?").getOrElse(""),
          helixLength = spl(12).filter(_ != "?").map(_.toInt)
        )
      }

    val sheet: Seq[PdbSheet] = {
      val structSheet = parseTable(
        "_struct_sheet_range",
        Vector(
          "id",
          "sheet_id",
          "beg_auth_comp_id",
          "beg_auth_asym_id",
          "beg_auth_seq_id",
          "pdbx_beg_PDB_ins_code",
          "end_auth_comp_id",
          "end_auth_asym_id",
          "end_auth_seq_id",
          "pdbx_end_PDB_ins_code"
        )
      ).map { spl =>
        val rangeId = spl(0).get.toInt
        val sheetid = spl(1).get
        val begComp = spl(2).get
        val begChain = spl(3).get
        val begNum = spl(4).get.toInt
        val begCode = spl(5).filter(_ != "?").getOrElse("")

        val endComp = spl(6).get
        val endChain = spl(7).get
        val endNum = spl(8).get.toInt
        val endCode = spl(9).filter(_ != "?").getOrElse("")

        ((rangeId, sheetid),
         (begComp,
          begChain,
          begNum,
          begCode,
          endComp,
          endChain,
          endNum,
          endCode))

      }.toMap

      val bonds = parseTable(
        "_pdbx_struct_sheet_hbond",
        Vector(
          "range_id_2",
          "sheet_id",
          "range_2_auth_atom_id",
          "range_2_auth_comp_id",
          "range_2_auth_asym_id",
          "range_2_auth_seq_id",
          "range_2_PDB_ins_code",
          "range_1_auth_atom_id",
          "range_1_auth_comp_id",
          "range_1_auth_asym_id",
          "range_1_auth_seq_id",
          "range_1_PDB_ins_code"
        )
      ).map { spl =>
        val rangeId = spl(0).get.toInt
        val sheetid = spl(1).get
        val curAtom = spl(2).get
        val curComp = spl(3).get
        val curChain = spl(4).get
        val curNum = spl(5).get.toInt
        val curCode = spl(6).filter(_ != "?").getOrElse("")

        val prevAtom = spl(7).get
        val prevComp = spl(8).get
        val prevChain = spl(9).get
        val prevNum = spl(10).get.toInt
        val prevCode = spl(11).filter(_ != "?").getOrElse("")

        ((rangeId, sheetid),
         (curAtom,
          curComp,
          curChain,
          curNum,
          curCode,
          prevAtom,
          prevComp,
          prevChain,
          prevNum,
          prevCode))
      }.toMap

      val sense = parseTable("_struct_sheet_order",
                             Vector("range_id_2", "sheet_id", "sense")).map {
        spl =>
          val rangeId = spl(0).get.toInt
          val sheetid = spl(1).get
          val sense = spl(2).get match {
            case "anti-parallel" => -1
            case "parallel"      => 1
          }
          (rangeId -> sheetid) -> sense
      }.toMap

      val numberOfSheets =
        parseTable("_struct_sheet", Vector("id", "number_strands")).map { spl =>
          val sheetid = spl(0).get
          val num = spl(1).get.toInt
          sheetid -> num
        }.toMap

      structSheet.map {
        case ((rangeId, sheetId),
              (begComp,
               betChain,
               begNum,
               begCode,
               endComp,
               endChain,
               endNum,
               endCode)) =>
          val bonds1 = bonds.get(rangeId -> sheetId)
          val senseV = sense.get(rangeId -> sheetId).getOrElse(0)
          val num = numberOfSheets(sheetId)

          PdbSheet(
            rangeId,
            sheetId,
            num,
            begComp,
            betChain,
            begNum,
            begCode,
            endComp,
            endChain,
            endNum,
            endCode,
            senseV,
            bonds1.map(_._1),
            bonds1.map(_._2),
            bonds1.map(_._3),
            bonds1.map(_._4),
            bonds1.map(_._5),
            bonds1.map(_._6),
            bonds1.map(_._7),
            bonds1.map(_._8),
            bonds1.map(_._9),
            bonds1.map(_._10)
          )
      }.toList

    }

    val _struct_ref_seq_dif
      : Map[(PdbChain, PdbResidueNumber), Option[String]] = {
      parseTable("_struct_ref_seq_dif",
                 Vector("pdbx_auth_seq_num",
                        "pdbx_pdb_strand_id",
                        "pdbx_seq_db_name",
                        "db_mon_id",
                        "pdbx_pdb_ins_code"))
        .map { spl =>
          val pdbChain = PdbChain(spl(1).get)
          val dbName = spl(2).get
          val uniprotmonomer = {
            val x = spl(3).getOrElse("?")
            if (x == "?") None else Some(x)
          }
          val pdbResidue = {
            val num = spl(0).filter(x => x != "?" && x != ".")
            val ic = spl(4).filter(_ != "?")
            num.map { num =>
              PdbResidueNumber(num.toInt, ic)
            }
          }
          ((pdbChain, pdbResidue), uniprotmonomer, dbName)
        }
        .filter(x => x._3 == "UNP" && x._1._2.isDefined)
        .map(x => (x._1._1 -> x._1._2.get) -> x._2)
        .toMap

    }

    val parsedAtomTable = parseTable(
      "_atom_site",
      Vector(
        "id",
        "auth_asym_id",
        "auth_seq_id",
        "pdbx_PDB_ins_code",
        "Cartn_x",
        "Cartn_y",
        "Cartn_z",
        "occupancy",
        "label_entity_id",
        "pdbx_PDB_model_num",
        "label_atom_id",
        "type_symbol",
        "auth_comp_id",
        "label_asym_id",
        "B_iso_or_equiv"
      )
    )

    val representativeModelNumber: Option[Int] = {
      val nmr_rep =
        parseTable("_pdbx_nmr_representative", Vector("conformer_id"))
          .map { spl =>
            spl(0)
          }
          .filter(_.isDefined)
          .map(_.get.toInt)
          .headOption

      val smallest =
        parsedAtomTable
          .map(_(9))
          .filter(_.isDefined)
          .map(_.get.toInt)
          .distinct
          .sorted
          .headOption

      nmr_rep.orElse(smallest)

    }

    val polyProteinEntities: Set[Int] =
      parseTable("_entity_poly", Vector("entity_id", "type"))
        .map { spl =>
          spl(0).get -> spl(1).get
        }
        .filter(_._2.contains("polypeptide"))
        .map(_._1.toInt)
        .toSet

    // _pdbx_struct_assembly.id
    // _pdbx_struct_assembly.details
    val assemblyDetails: Map[PdbAsssemblyId, String] =
      parseTable("_pdbx_struct_assembly", Vector("id", "details"))
        .filter(_.forall(_.isDefined))
        .map { spl =>
          (PdbAsssemblyId(spl(0).get), spl(1).get)
        }
        .toMap

    val assemblyId: Option[PdbAsssemblyId] = assemblyDetails.toSeq
      .sortBy(_._1.i)
      .find(_._2.contains("author"))
      .orElse(assemblyDetails.toSeq.sortBy(_._1.i).headOption)
      .map(_._1)

    val baseOperations: Map[String, AffineTransformation] =
      parseTable(
        "_pdbx_struct_oper_list",
        Vector(
          "id",
          "matrix[1][1]",
          "matrix[1][2]",
          "matrix[1][3]",
          "vector[1]",
          "matrix[2][1]",
          "matrix[2][2]",
          "matrix[2][3]",
          "vector[2]",
          "matrix[3][1]",
          "matrix[3][2]",
          "matrix[3][3]",
          "vector[3]"
        )
      ).filter(_.forall(_.isDefined))
        .map { spl =>
          val id = spl(0).get
          val m11 = spl(1).get.toDouble
          val m12 = spl(2).get.toDouble
          val m13 = spl(3).get.toDouble
          val v1 = spl(4).get.toDouble
          val m21 = spl(5).get.toDouble
          val m22 = spl(6).get.toDouble
          val m23 = spl(7).get.toDouble
          val v2 = spl(8).get.toDouble
          val m31 = spl(9).get.toDouble
          val m32 = spl(10).get.toDouble
          val m33 = spl(11).get.toDouble
          val v3 = spl(12).get.toDouble
          val mat =
            Mat(3, 3, Array(m11, m12, m13, m21, m22, m23, m31, m32, m33))
          val b = Vec(v1, v2, v3)
          id -> AffineTransformation(mat, b)

        }
        .toMap

    def parseOperations(s: String): Seq[AffineTransformation] = {

      def parseRange(s: String): List[String] =
        s.split(",")
          .flatMap { r =>
            val sp = r.split("-")
            scala.util
              .Try {
                val from = sp(0).toInt
                val to = if (sp.size == 2) sp(1).toInt else from
                (from to to).toList.map(_.toString)
              }
              .toOption
              .getOrElse(List(sp(0)))
          }
          .toList

      val ranges =
        s.split("\\)\\(")
          .map(
            _.stripPrefix("'")
              .stripPrefix("(")
              .stripSuffix("'")
              .stripSuffix(")"))
          .map(parseRange)
      if (ranges.size == 1)
        ranges.head.map(i => baseOperations(i))
      else {
        val h = ranges(0)
        val t = ranges(1)
        t.flatMap { t =>
          h.map { h =>
            baseOperations(t).andThen(baseOperations(h))

          }
        }
      }

    }

    val operations
      : Map[PdbAsssemblyId, Map[CifChain, Seq[AffineTransformation]]] =
      parseTable("_pdbx_struct_assembly_gen",
                 Vector("assembly_id", "asym_id_list", "oper_expression"))
        .filter(_.forall(_.isDefined))
        .map { spl =>
          (PdbAsssemblyId(spl(0).get),
           (spl(1).get.split1(',').map(i => CifChain(i)),
            parseOperations(spl(2).get)))
        }
        .groupBy(_._1)
        .map {
          case (assemblyId, values) =>
            val chains: Seq[(CifChain, Seq[AffineTransformation])] =
              values.map(_._2).flatMap(y => y._1.map(_ -> y._2))

            assemblyId -> chains
              .groupBy(_._1)
              .map(x => x._1 -> (x._2.map(_._2).reduce(_ ++ _)))
        }

    val rawAtomList: Vector[(Atom, PdbChain, PdbResidueNumber, CifChain)] =
      parsedAtomTable
        .map { spl =>
          val id = spl(0).get.toInt
          val chain = PdbChain(spl(1).get)
          val rn = spl(2).get.toInt
          val insC = if (spl(3).isEmpty) "?" else spl(3).get
          val pdbResidue =
            PdbResidueNumber(rn, (if (insC == "?") None else Some(insC)))
          val coord =
            Vec(spl(4).get.toDouble, spl(5).get.toDouble, spl(6).get.toDouble)
          val occ = spl(7).get.toDouble
          val entityId = spl(8).get.toInt
          val cifChain = CifChain(spl(13).getOrElse(chain.s))
          val polyProtein = polyProteinEntities.contains(entityId)
          val modelNumber: Option[Int] = spl(9).map(_.toInt).orElse(None)

          (chain,
           pdbResidue,
           Atom(coord,
                id,
                name = spl(10).getOrElse(""),
                symbol = spl(11).getOrElse(""),
                residue3 = spl(12).getOrElse(""),
                tempFactor = spl(14).getOrElse("")),
           occ,
           polyProtein,
           modelNumber == representativeModelNumber,
           cifChain)
        }
        .filter(x => x._4 > 0.0 && x._5 && x._6)
        .map(x => (x._3, x._1, x._2, x._7))
        .toVector

    val assemblyOperations: Map[CifChain, Seq[AffineTransformation]] =
      assemblyId.flatMap(operations.get).getOrElse {
        val chains = rawAtomList.map(_._4).toSet
        chains.toList.map { chain =>
          chain -> List(AffineTransformation.identity)
        }.toMap
      }
    val assemblyChainsSet: Set[CifChain] = assemblyOperations.keySet.toSet

    val aminoAcidSequence = {

      val isHeader = (s: String) => s.startsWith("_atom_site.")
      val isAtom = (s: String) => s.startsWith("ATOM")
      val isHetAtom = (s: String) => s.startsWith("HETATM")

      val lines =
        allLines.filter(x => isHeader(x) || isAtom(x) || isHetAtom(x)).toList
      val header = lines.filter(isHeader).map(_.trim)
      val atoms = lines.filter(x => isAtom(x) || isHetAtom(x))
      val residueIdx = header.indexOf("_atom_site.auth_comp_id")
      val chainIdx = header.indexOf("_atom_site.auth_asym_id")
      val polyseqNumIdx = header.indexOf("_atom_site.label_seq_id")
      val residueNumberIdx = header.indexOf("_atom_site.auth_seq_id")
      val insertionCodeIdx = header.indexOf("_atom_site.pdbx_PDB_ins_code")
      val entityIdIdx = header.indexOf("_atom_site.label_entity_id")
      val modelNumberIdx = header.indexOf("_atom_site.pdbx_PDB_model_num")
      val labelAsymIdIdx = header.indexOf("_atom_site.label_asym_id")
      val mIdx =
        List(residueIdx,
             chainIdx,
             residueNumberIdx,
             insertionCodeIdx,
             polyseqNumIdx).max
      atoms
        .map { line =>
          line.splitM(' ')
        }
        .filter(_.size >= mIdx)
        .filter(_(residueNumberIdx) != ".")
        .map { spl =>
          val entityId = spl(entityIdIdx).toInt
          val polyProtein = polyProteinEntities.contains(entityId)
          val threeLetterInAtomTable = spl(residueIdx)
          val rn = spl(residueNumberIdx).toInt
          val insC = if (insertionCodeIdx == -1) "?" else spl(insertionCodeIdx)
          val pdbResidue =
            PdbResidueNumber(rn, (if (insC == "?") None else Some(insC)))
          val ch = PdbChain(spl(chainIdx))
          val cifChain =
            if (labelAsymIdIdx >= 0) CifChain(spl(labelAsymIdIdx))
            else CifChain(ch.s)
          val threeLetterInUniProt =
            _struct_ref_seq_dif.get((ch -> pdbResidue)) match {
              case None          => Some(threeLetterInAtomTable) // no override
              case Some(None)    => None // override, remove
              case Some(Some(x)) => Some(x) // override change code
            }

          val oneLetter =
            threeLetterInUniProt.map(t => three2One.get(t).getOrElse('X'))

          val modelNumber: Option[Int] =
            if (modelNumberIdx < 0) None else Some(spl(modelNumberIdx).toInt)

          (oneLetter,
           pdbResidue,
           ch,
           polyProtein,
           modelNumber == representativeModelNumber,
           cifChain)

        }
        .filter(x =>
          x._4 && x._1.isDefined && assemblyChainsSet.contains(x._6) && x._4)
        .map(x => (x._1.get, x._2, x._3))
        .distinct
        .groupBy(_._3)
        .map(x => x._1 -> x._2.map(x => (x._1, x._2)).sortBy(_._2))
    }

    val assembly: Structure = {

      if (assemblyId.isEmpty)
        Structure(
          rawAtomList.map(x => (x._1, x._2, x._3, FakePdbChain(x._2.s))),
          Map(),
          helix,
          sheet)
      else {
        var atomIdx = 0
        val atomsGroupedByChain = rawAtomList.groupBy(_._4: CifChain)
        val chains: Map[(CifChain, Int), String] =
          atomsGroupedByChain.toSeq
            .flatMap {
              case (cifChain, _) =>
                assemblyOperations
                  .get(cifChain)
                  .toList
                  .flatten
                  .zipWithIndex
                  .map(o => (cifChain, o._2))
            }
            .zipWithIndex
            .map {
              case (k, i) => k -> (97 + i).toChar.toString.toUpperCase
            }
            .toMap
        val cif2pdbchain: Map[CifChain, PdbChain] =
          rawAtomList.map(x => x._4 -> x._2).distinct.toMap

        val pdbchainRemap: Map[FakePdbChain, PdbChain] = chains.map {
          case ((cif, _), newchain) =>
            val pdbchain = cif2pdbchain(cif)
            FakePdbChain(newchain) -> pdbchain
        }.toMap
        val mappedAtomList = atomsGroupedByChain.flatMap {
          case (cifChain, atoms) =>
            assemblyOperations
              .get(cifChain)
              .toList
              .flatten
              .zipWithIndex
              .flatMap {
                case (operation, opidx) =>
                  atoms.map {
                    case (atom, chain, residue, _) =>
                      atomIdx += 1
                      val imageAtom: Atom =
                        Atom(operation.apply(atom.coord),
                             atomIdx,
                             atom.name,
                             atom.symbol,
                             atom.residue3,
                             atom.tempFactor)
                      (imageAtom,
                       chain,
                       residue,
                       FakePdbChain(chains(cifChain -> opidx)))
                  }

              }
        }.toVector

        Structure(mappedAtomList, pdbchainRemap, helix, sheet)
      }
    }

    CIFContents(aminoAcidSequence, assembly)
  }
}
