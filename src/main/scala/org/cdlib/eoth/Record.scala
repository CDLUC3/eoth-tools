package org.cdlib.eoth

import java.io.File
import java.net.URI
import java.nio.file.{Paths, Path}
import java.time.LocalDate

import org.apache.log4j.Logger

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.xml._

object Record {

  val log = Logger.getLogger(Record.getClass)

  val DC_TERMS = List("abstract", "accessRights", "accrualMethod", "accrualPeriodicity", "accrualPolicy", "alternative", "audience", "available", "bibliographicCitation", "conformsTo", "contributor", "coverage", "created", "creator", "date", "dateAccepted", "dateCopyrighted", "dateSubmitted", "description", "educationLevel", "extent", "format", "hasFormat", "hasPart", "hasVersion", "identifier", "instructionalMethod", "isFormatOf", "isPartOf", "isReferencedBy", "isReplacedBy", "isRequiredBy", "isVersionOf", "issued", "language", "license", "mediator", "medium", "modified", "provenance", "publisher", "references", "relation", "replaces", "requires", "rights", "rightsHolder", "source", "spatial", "subject", "tableOfContents", "temporal", "title", "type", "valid")

  def handle(f: File): Map[String, Seq[String]] = {
    print('.')
    val xml = XML.loadFile(f)
    DC_TERMS.map({ dc =>
      dc -> (xml \ dc).map(n => n.text)
    }).toMap
  }

  def main(args: Array[String]) {
    val eothxtf = Paths.get("").resolve("eothxtf")
    log.debug(s"eothxtf directory: $eothxtf")
    val data = eothxtf.resolve("data")
    val eoth08 = data.resolve("eoth08")
    val eoth12 = data.resolve("eoth12")

    log.debug(s"eoth08 directory: $eoth08")
    log.debug(s"eoth12 directory: $eoth12")

    var handleCount = 0
    val results: Stream[(String, Object)] = Stream(eoth08, eoth12).flatMap(p => p.toFile().listFiles().filter(f => f.getName.endsWith(".xml"))).map({ f: File =>
      handleCount = handleCount + 1
      if (handleCount % 100 == 0) {
        println(s" $handleCount")
      }
      try {
        f.getAbsolutePath -> handle(f)
      } catch {
        case e: Exception =>
          f.getAbsolutePath -> e
      }
    })

    val allCounts = mutable.Map[String, List[Int]]()
    val exceptions = mutable.Map[String, Exception]()

    results.foreach {
      case (file, termToValue: Map[String, Seq[String]]) =>
        DC_TERMS.foreach({ term: String =>
          val count = termToValue(term).size
          val counts = allCounts.getOrElse(term, List[Int]())
          allCounts(term) = counts :+ count
        })
      case (s, e: Exception) =>
        exceptions(s) = e
    }

    println(s" $handleCount")

    println("------------------------------------------------------------------------------------------------------------------------")

    var dcTermsUsed = List[String]()

    DC_TERMS.foreach({ dc: String =>
      val counts = allCounts.getOrElse(dc, List[Int]())
      val max = counts.max
      if (max > 0) {
        dcTermsUsed = dcTermsUsed :+ dc
        println(s"$dc: $max")
      }
    })

    println("------------------------------------------------------------------------------------------------------------------------")

    val valuesForTerms = mutable.Map[String, Set[String]]()
    results.foreach {
      case (file, termToValue: Map[String, Seq[String]]) =>
        dcTermsUsed.foreach({ term =>
          termToValue.get(term) match {
            case Some(values) =>
              val allValues = valuesForTerms.getOrElse(term, Set[String]())
              valuesForTerms(term) = allValues ++ values
            case _ =>
          }
        })
      case _ =>
    }

    println("------------------------------------------------------------------------------------------------------------------------")

    val term_to_max = mutable.Map[String, Int]()

    println("| term | appears in | min occurrences | max occurrences | median occurrences | unique values |")
    println("| :--- | :--------- | :-------------- | :-------------- | :----------------- | :------------ |")
    dcTermsUsed.foreach({ term =>
      val appears_in = allCounts(term).count(p => p > 0)
      val sorted = allCounts(term).sorted
      val min_occurrences = sorted.min
      val max_occurrences = sorted.last
      val median_occurrences = sorted(sorted.size / 2)
      val uniqueValues = valuesForTerms(term).size
      term_to_max(term) = max_occurrences
      println("| %s | %d | %d | %d | %d | %d |".format(term, appears_in, min_occurrences, max_occurrences, median_occurrences, uniqueValues))
    })

    if (exceptions.nonEmpty) {
      println("------------------------------------------------------------------------------------------------------------------------")
      val byMessage: mutable.Map[Exception, String] = exceptions.map { case (s: String, e: Exception) => (e, s) }
      byMessage.keys.toList.sortBy(e => e.getMessage).foreach({ e =>
        val s = byMessage(e)
        println("%s\t%s".format(s, e.getMessage))
      })
    }
  }
}
