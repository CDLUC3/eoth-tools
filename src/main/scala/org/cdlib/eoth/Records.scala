package org.cdlib.eoth

import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.time.{LocalDateTime, LocalDate}

import org.apache.log4j.Logger

class Records(val records: List[Record]) {

  lazy val withCoverage: List[Record] = {
    records.filter(_.hasCoverage)
  }

  lazy val withoutCoverage: List[Record] = {
    records.filter(!_.hasCoverage)
  }

  lazy val withDescription: List[Record] = {
    records.filter(_.hasDescription)
  }

  lazy val withoutDescription: List[Record] = {
    records.filter(!_.hasDescription)
  }

  lazy val withSubject: List[Record] = {
    records.filter(_.hasSubject)
  }

  lazy val withoutSubject: List[Record] = {
    records.filter(!_.hasSubject)
  }

  lazy val withTitle: List[Record] = {
    records.filter(_.hasTitle)
  }

  lazy val withoutTitle: List[Record] = {
    records.filter(!_.hasTitle)
  }

  lazy val withFile: List[Record] = {
    records.filter(_.hasFile)
  }

  lazy val withoutFile: List[Record] = {
    records.filter(!_.hasFile)
  }

  lazy val byCoverage: Map[String, Set[Record]] = {
    val tuples = withCoverage.map(r => r.coverage.get -> r)
    toMultimap(tuples)
  }

  lazy val byDate: Map[Either[LocalDate, LocalDateTime], Set[Record]] = {
    toMultimap(records.flatMap(r => r.date.map(d => d -> r)))
  }

  lazy val byDescription: Map[String, Set[Record]] = {
    toMultimap(withDescription.flatMap(r => r.description.map(d => d -> r)))
  }

  lazy val byFormat: Map[String, Set[Record]] = {
    toMultimap(records.map(r => r.format -> r))
  }

  lazy val byIdentifier: Map[URI, Set[Record]] = {
    toMultimap(records.map(r => r.identifier -> r))
  }

  lazy val byProvenance: Map[URI, Set[Record]] = {
    toMultimap(records.map(r => r.provenance -> r))
  }

  lazy val byRelation: Map[URI, Set[Record]] = {
    toMultimap(records.map(r => r.relation -> r))
  }

  lazy val bySource = {
    toMultimap(records.map(r => r.source -> r))
  }

  lazy val bySubject = {
    toMultimap(records.flatMap(r => r.subject.map(s => s -> r)))
  }

  lazy val byTitle = {
    toMultimap(withTitle.flatMap(r => r.title.map(t => t -> r)))
  }

  lazy val byType = {
    toMultimap(records.map(r => r.`type` -> r))
  }

  lazy val withLongestSubject: Record = {
    withSubject.sortBy({r =>
      r.longestSubject.length
    }).last
  }

  lazy val byNumSubjects: List[Record] = {
    records.sortBy(_.numSubjects)
  }

  lazy val withMostSubjects: Record = {
    byNumSubjects.last
  }

  // --------------------------------------------------
  // Private

  private def toMultimap[K](tuples: List[(K, Record)]): Map[K, Set[Record]] = {
    tuples.groupBy(_._1).map {
      case (k: K, records: List[(K, Record)]) =>
        k -> records.map(_._2).toSet
    }
  }
}

object Records {
  val log = Logger.getLogger(Records.getClass)

  def main(args: Array[String]) {
    val eothxtf = Paths.get("").resolve("eothxtf")
    log.debug(s"eothxtf directory: $eothxtf")
    val data = eothxtf.resolve("data")
    val eoth08 = data.resolve("eoth08").toFile
    val eoth12 = data.resolve("eoth12").toFile

    Stream(eoth08, eoth12).foreach {d =>
      val xmlFiles = d.listFiles().filter(_.getName.endsWith(".xml"))

      var handleCount = 0
      val records = new Records(xmlFiles.flatMap({ f: File =>
        handleCount = handleCount + 1
        if (handleCount % 100 == 0) {
//          println(s" $handleCount")
        }
        Record.fromFile(f)
      }).toList)

//      println(handleCount)
//      println()

      println("#### Longest subject (" + d.getName + "):")
      println()
      val withLongestSubject = records.withLongestSubject
      println("**File:** `" + withLongestSubject.relativePath + "`")
      println()
      println("**ID:** `" + withLongestSubject.identifier + "`")
      println()
      println("**Subject:**")
      println()
      println("- " + withLongestSubject.longestSubject)

      println()

      println("#### Most subjects (" + d.getName + "):")
      println()
      val withMostSubjects = records.withMostSubjects
      println("**File:** `" + withMostSubjects.relativePath + "`")
      println()
      println("**ID:** `" + withMostSubjects.identifier + "`")
      println()
      println("**Subjects:**")
      println()
      withMostSubjects.subject.foreach { s =>
        println("- " + s)
      }

      println()
    }
  }
}
