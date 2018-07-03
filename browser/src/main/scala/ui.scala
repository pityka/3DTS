package sd.ui

import sd._
import scala.scalajs._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js._
import scala.scalajs.js.annotation._
import org.scalajs.dom.ext.Ajax

import framework.Framework._
import org.scalajs.dom.raw._
import rx._
import scalatags.JsDom.all._

class ProteinUI(
    parentNode: Node
)(implicit ctx: Ctx.Owner) {

  def renderProtein(
      pdbId: String,
      colors: Map[(PdbChain, PdbResidueNumberUnresolved), Array[Double]] = Map(),
      onClick: (String, String, Int, js.Dynamic, js.Dynamic, String) => Unit =
        (_, _, _, _, _, _) => ()) = {
    val colorByResidueIdentityRgb =
      (callback: (String, Int) => Array[Double]) => {
        js.Dynamic.newInstance(js.Dynamic.global.pv.color.ColorOp)(
          ((atom: js.Dynamic, out: Array[Double], index: Int) => {
            val chainName =
              atom.residue().chain().name().asInstanceOf[String]
            val residueNumber = atom.residue().num().asInstanceOf[Int]
            val rgb = callback(chainName, residueNumber)
            out(index + 0) = rgb(0)
            out(index + 1) = rgb(1)
            out(index + 2) = rgb(2)
            out(index + 3) = 1.0
          }))
      }

    val viewContainer = div().render
    val options = js.Dynamic.literal(width = 1200,
                                     height = 600,
                                     antialias = true,
                                     quality = "medium")
    val viewer = js.Dynamic.global.pv.Viewer(viewContainer, options)

    Ajax.get("pdb/" + pdbId).map(_.responseText).map { pdbtext =>
      val chainRemap = scala.io.Source
        .fromString(pdbtext)
        .getLines
        .next
        .drop("REMARK 9 ".size)
        .trim
        .split(":")
        .grouped(2)
        .map(x => x(0) -> x(1))
        .toMap
      val chainRemapReverse =
        chainRemap.toSeq.groupBy(_._2).map(x => x._1 -> x._2.map(_._1))
      val fetchedStructure =
        js.Dynamic.global.pv.io.pdb(pdbtext)
      val renderedStructure = viewer.cartoon("protein", fetchedStructure)
      viewer.centerOn(fetchedStructure)

      UIState.viewerAndStructure() =
        Some((viewer, fetchedStructure, chainRemapReverse))

      viewer.addListener(
        "click",
        (picked: js.Dynamic) => {
          if (picked == null) ()
          else {
            val target = picked.target()
            if (!js.isUndefined(target.qualifiedName)) {
              val structureChainName =
                target.residue().chain().name().asInstanceOf[String]
              val chainName: String =
                chainRemap(structureChainName)
              val residueNumber: Int =
                target.residue().num().asInstanceOf[Int]
              onClick(pdbId,
                      chainName,
                      residueNumber,
                      viewer,
                      fetchedStructure,
                      structureChainName)
            }
          }
        }
      )
      renderedStructure.colorBy(colorByResidueIdentityRgb(
        (chain: String, res: Int) => {
          colors
            .get(PdbChain(chainRemap(chain)) -> PdbResidueNumberUnresolved(
              res.toString))
            .getOrElse(Array(0.9, 0.9, 0.9))
        }
      ))
    }

    (viewContainer)
  }

  case class ScoreSelector(score: DepletionRow => Double,
                           cdf: DepletionScoreCDFs => Seq[(Double, Double)])
  object ScoreSelector {
    val globalSynonymous = ScoreSelector(_.nsPostGlobalSynonymousRate.post.mean,
                                         _.nsPostMeanGlobalSynonymousRate)

    val heptamerSpecificIntergenic = ScoreSelector(
      _.nsPostHeptamerSpecificIntergenicRate.post.mean,
      _.nsPostMeanHeptamerSpecificIntergenicRate)

    val heptamerIndependentIntergenic = ScoreSelector(
      _.nsPostHeptamerIndependentIntergenicRate.post.mean,
      _.nsPostMeanHeptamerIndependentIntergenicRate)

    val heptamerSpecificChromosomeSpecificIntergenic = ScoreSelector(
      _.nsPostHeptamerSpecificChromosomeSpecificIntergenicRate.post.mean,
      _.nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate)

    val heptamerIndependentChromosomeSpecificIntergenic = ScoreSelector(
      _.nsPostHeptamerIndependentChromosomeSpecificIntergenicRate.post.mean,
      _.nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate)

  }

  class UIState(implicit ctx: Ctx.Owner) {

    val cdfs: Var[Option[DepletionScoreCDFs]] = Var(None)

    val selectedScore: Var[ScoreSelector] = Var(
      ScoreSelector.heptamerIndependentChromosomeSpecificIntergenic)

    Server.getCdfs.foreach { d =>
      cdfs() = Some(d)
    }

    val currentData =
      Var[(Seq[PdbId], Seq[DepletionScoresByResidue])]((Nil, Nil))

    val byResidue
      : Rx[Map[(PdbChain, PdbResidueNumberUnresolved), Seq[DepletionRow]]] =
      currentData.map(
        _._2
          .groupBy(x =>
            (PdbChain(x.pdbChain) -> PdbResidueNumberUnresolved(x.pdbResidue)))
          .map { x =>
            x._1 -> x._2.map(_.featureScores).distinct
          })

    val clicked =
      Var[Option[(PdbId, PdbChain, PdbResidueNumberUnresolved)]](None)

    val waitState = Var(false)

    val lastQuery: Var[Option[String]] = Var(None)

    val viewerAndStructure =
      Var[Option[(js.Dynamic, js.Dynamic, Map[String, Seq[String]])]](None)

  }

  private val UIState = new UIState

  def makeQuery(q: String) = {
    UIState.waitState() = true
    UIState.lastQuery() = Some(q)
    Server.query(q).map { data =>
      UIState.waitState() = false
      UIState.currentData() = data
      UIState.clicked() = None
    }
  }

  val scoreSelectorInput = select(
    style := "border: 1px solid #ddd",
    height := "30",
    width := "100"
  )(
    option(value := "s5", selected := true)(
      "Non coding heptamer independent chromosome specific variation"),
    option(value := "s1")("Synonymous Coding variation"),
    option(value := "s2")("Non coding heptamer specific variation"),
    option(value := "s3")("Non coding heptamer independent variation"),
    option(value := "s4")(
      "Non coding heptamer specific chromosome specific variation")
  ).render

  scoreSelectorInput.onchange = (e: Event) => {
    UIState.waitState() = true
    scoreSelectorInput.value match {
      case "s1" => UIState.selectedScore() = ScoreSelector.globalSynonymous
      case "s2" =>
        UIState.selectedScore() = ScoreSelector.heptamerSpecificIntergenic
      case "s3" =>
        UIState.selectedScore() = ScoreSelector.heptamerIndependentIntergenic
      case "s4" =>
        UIState.selectedScore() =
          ScoreSelector.heptamerSpecificChromosomeSpecificIntergenic
      case "s5" =>
        UIState.selectedScore() =
          ScoreSelector.heptamerIndependentChromosomeSpecificIntergenic
    }

  }

  val queryBox = input(
    `class` := "uk-search-input",
    style := "border: 1px solid #ddd",
    `type` := "text",
    height := "30",
    width := "100",
    placeholder := "You can search for PDP identifier, UniProt ID, Ensemble Transcript ID, hg38 `chromosome_position`, `pdbid_pdbchain` , `pdbid_pdbchain_pdbresidue`  "
  ).render
  queryBox.onkeypress = (e: KeyboardEvent) => {
    if (e.keyCode == 13) {
      makeQuery(queryBox.value)
    }
  }

  val downloadLink = Rx {
    if (UIState.currentData()._2.isEmpty || UIState.lastQuery().isEmpty) div()
    else {
      div(
        a(href := "/query?format=csv&q=" + UIState.lastQuery().get)(
          "Download as csv"))
    }
  }

  val waitIndicator = Rx {
    val w = UIState.waitState()
    if (w) div("Wait..") else div()
  }

  val resolvedPDBs = Rx {
    val data = UIState.currentData()._1
    if (data.nonEmpty)
      span("Your query (also) returned the following pdb identifiers: ")(
        data.flatMap(pdb =>
          List(a(onclick := { (event: Event) =>
            makeQuery(pdb.s)
          })(pdb.s), span(", "))))
    else span()

  }

  // val mappingTable = Rx {
  //   val data = UIState.currentData()._1
  //   println("update mapping table")

  //   table(`class` := "uk-table")(
  //     thead(td("PDB"),
  //           td("chain"),
  //           td("Res."),
  //           td("Pdb aa"),
  //           td("UniProt"),
  //           td("Uni.offset"),
  //           td("Uni aa"),
  //           td("ENST"),
  //           td("locus"),
  //           td("idx codon"),
  //           td("idx trscpt"),
  //           td("cons"),
  //           td("ref")))(data.sortBy(_._9.s.split("\\t").last.toInt).map {
  //     case (PdbId(pdbid),
  //           PdbChain(chain),
  //           PdbResidueNumberUnresolved(pdbres),
  //           PdbSeq(pdbaa),
  //           UniId(uniid),
  //           UniNumber(uninum),
  //           UniSeq(uniaa),
  //           EnsT(enst),
  //           ChrPos(cp),
  //           IndexInCodon(idxCod),
  //           IndexInTranscript(idxtr),
  //           MissenseConsequences(cons),
  //           _,
  //           RefNuc(ref),
  //           _,
  //           _) =>
  //       tr(td(pdbid),
  //          td(chain),
  //          td(pdbres),
  //          td(pdbaa),
  //          td(uniid),
  //          td(uninum),
  //          td(uniaa),
  //          td(enst),
  //          td(cp),
  //          td(idxCod),
  //          td(idxtr),
  //          td(cons.map(x => x._1 + ":" + x._2).mkString("|")),
  //          td(ref.toString))
  //   })

  // }

  def renderTable(click: String,
                  scores: Seq[DepletionRow],
                  scoreSelector: ScoreSelector) =
    table(`class` := "uk-table uk-table-striped uk-table-hover uk-table-small")(
      thead(td("clicked"),
            td("remarks"),
            td("feature"),
            td("obsNS"),
            td("expNs"),
            td("obsS"),
            td("expS"),
            td("size"),
            td("score"),
            // td("postmean_2"),
            td("uniprot")))(scores.map {
      case drow @ DepletionRow(feature,
                               ObsNs(obsns),
                               ExpNs(expns),
                               ObsS(obss),
                               ExpS(exps),
                               NumLoci(size),
                               NsPostGlobalSynonymousRate(_),
                               _,
                               _,
                               _,
                               _,
                               unis) =>
        val btn = button(
          `class` := "uk-button uk-button-default uk-button-small uk-button-primary",
          `type` := "button")("Send").render
        val txt = textarea(`class` := "uk-textarea",
                           placeholder := "Your feedback..",
                           cols := 50,
                           rows := 8).render
        val toggle =
          button(`class` := "uk-button uk-button-default uk-button-small",
                 `type` := "button")("Feedback").render
        val formElem = form(style := "display:none")(
          txt,
          btn
        ).render
        var shown = false
        toggle.onclick = { (e: Event) =>
          if (shown == true) {
            shown = false
            formElem.style.display = "none"
            toggle.innerHTML = "Feedback"
          } else {
            shown = true
            formElem.style.display = "block"
            toggle.innerHTML = "close"
          }
        }

        btn.onclick = { (e: Event) =>
          Server.post(txt.value, feature.toString).onComplete {
            case _ =>
              shown = false
              formElem.style.display = "none"
              toggle.innerHTML = "Feedback"
          }
        }

        val featureElem =
          td(feature.toString, small("(Click to center)")).render

        val nsPostmean = scoreSelector.score(drow)

        val row = tr(
          td(click),
          td(
            div(
              toggle,
              formElem
            )),
          featureElem,
          td(obsns),
          td(f"$expns%1.2f"),
          td(obss),
          td(f"$exps%1.2f"),
          td(size),
          td(f"$nsPostmean%1.2f"),
          //  td(f"$postmean2d%1.2f"),
          unis.map(_.s).mkString(",")
        ).render
        featureElem.onclick = {
          (e: Event) =>
            import js.JSConverters._
            val ch: PdbChain = feature.pdbChain
            val res: PdbResidueNumberUnresolved =
              feature.asInstanceOf[FeatureKey].pdbResidueMin
            UIState.viewerAndStructure.foreach {
              case Some((viewer, structure, chainRemapReverse)) =>
                val focusResidue =
                  structure.select(
                    js.Dynamic.literal(chains =
                                         chainRemapReverse(ch.s).toJSArray,
                                       rnum = res.s.toInt))
                viewer.ballsAndSticks("focus", focusResidue)
                viewer.centerOn(focusResidue)
              case None =>
            }
        }
        row
    })

  val clickedTable = Rx {
    val byResidue
      : Map[(PdbChain, PdbResidueNumberUnresolved), Seq[DepletionRow]] =
      UIState.byResidue()
    val scoreSelector = UIState.selectedScore()
    UIState
      .clicked()
      .map {
        case (pdbId, pdbChain, pdbRes) =>
          val scores =
            byResidue.get((pdbChain -> pdbRes)).toList.flatten.distinct
          renderTable(pdbId.s + "/" + pdbChain.s + "/" + pdbRes.s,
                      scores,
                      scoreSelector)
      }
      .getOrElse(
        renderTable("", byResidue.values.flatten.toSeq.distinct, scoreSelector))
  }

  val resetClickButton =
    button(`class` := "uk-button uk-button-text uk-button-small")(
      "Clear selection").render
  resetClickButton.onclick = { (e: Event) =>
    UIState.clicked() = None
  }

  val colorkey = Rx {
    val cdfs = UIState.cdfs()
    val scoreSelector = UIState.selectedScore()
    val cdf = cdfs
      .map(scoreSelector.cdf)
      .getOrElse(Nil)
    val colorkeyData = ((1 to 9) map { i =>
      i / 10d
    }).map { value =>
      val valueInCdf = cdf
        .sliding(2, 1)
        .find {
          case l =>
            l(0)._1 <= value && value <= l(1)._1
        }
        .map(_.head._2)
        .getOrElse(0d)

      val color = org.nspl.HeatMapColors().apply(1 - valueInCdf)
      (value, (color.r, color.g, color.b))
    }

    div(style := "display:flex;flex-direction:column;")(
      div(style := "display:flex;flex-direction:row")(colorkeyData.map {
        case (_, (r, g, b)) =>
          div(
            style := s"height:10px;width:30px;background-color: rgb($r,$g,$b);")
      }),
      div(
        style := "display:flex;flex-direction:row;justify-content:space-between; width:300px;")(
        colorkeyData.map {
          case (value, _) => span(style := "width:30px")(value.toString)
        })
    )

  }

  val proteinView = Rx {
    val data = UIState.currentData()
    val cdfs = UIState.cdfs()
    val scoreSelector = UIState.selectedScore()
    val byResidue
      : Map[(PdbChain, PdbResidueNumberUnresolved), Seq[DepletionRow]] =
      UIState.byResidue()

    def colorByResidue(selector: DepletionRow => Double,
                       cdf: Seq[(Double, Double)])
      : Map[(PdbChain, PdbResidueNumberUnresolved), Array[Double]] = {
      byResidue.map {
        case (key, depletionRows) =>
          val value =
            depletionRows.map(depletionRow => selector(depletionRow)).min
          val valueInCdf = cdf
            .sliding(2, 1)
            .find {
              case l =>
                l(0)._1 <= value && value <= l(1)._1
            }
            .map(_.head._2)
            .getOrElse(0d)

          val color = org.nspl.HeatMapColors().apply(1 - valueInCdf)
          key -> Array(color.r / 255d, color.g / 255d, color.b / 255d)
      }
    }

    val colorByResidue_Mean1DLocal =
      colorByResidue(
        scoreSelector.score,
        cdfs
          .map(scoreSelector.cdf)
          .getOrElse(Nil)
      )

    val rendered = data._2.headOption
      .map(_.pdbId)
      .map { pdbId =>
        val viewContainer = renderProtein(
          pdbId,
          colorByResidue_Mean1DLocal,
          onClick = (pdb: String,
                     chainName: String,
                     residueNumber: Int,
                     viewer: js.Dynamic,
                     structure: js.Dynamic,
                     structureChainName) => {
            // format: off
            val focusResidue = structure.select(js.Dynamic.literal( chain = structureChainName, rnum= residueNumber ))
            viewer.ballsAndSticks("focus", focusResidue)
            viewer.centerOn(focusResidue)
            // format: on
            UIState.clicked() = Some(
              (PdbId(pdb),
               PdbChain(chainName),
               PdbResidueNumberUnresolved(residueNumber.toString)))
          }
        )

        div(style := "border:1px solid #ddd;")(viewContainer)
      }
      .getOrElse(div())

    UIState.waitState() = false

    rendered

  }

  val ui =
    div(
      // renderProtein("3DZY"),
      div(queryBox, waitIndicator, scoreSelectorInput),
      div(resolvedPDBs),
      div(style := "display:flex; flex-direction: column")(
        h3(`class` := "uk-heading")("Protein view"),
        downloadLink,
        div(style := "width: 80%")(div(proteinView),
                                   colorkey,
                                   resetClickButton),
        div(
          h3(`class` := "uk-heading")("Depletion scores in the protein"),
          clickedTable
        )
      )
    ).render

  parentNode.appendChild(ui)

}

@JSExportTopLevel("sd.ui.ProteinUIApp")
object ProteinUIApp {

  implicit val c = Ctx.Owner.safe()

  @JSExport
  def bind(parent: Node) = {

    new ProteinUI(parent)

  }

}
