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
    toMultimap(records.flatMap(r => r.source.map(s => s -> r)))
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
    withSubject.sortBy({ r =>
      r.longestSubject.length
    }).last
  }

  lazy val byNumSubjects: List[Record] = {
    records.sortBy(_.numSubjects)
  }

  lazy val withMostSubjects: Record = {
    byNumSubjects.last
  }

  lazy val allCoverage = byCoverage.keys.toList.sorted

  lazy val allSources = records.flatMap(r => r.source).distinct.sorted

  lazy val coverageToSource: Map[String, Set[String]] = allCoverage.map { coverage =>
    coverage -> byCoverage(coverage).flatMap(_.source).toSet
  }.toMap

  lazy val sourcesWithoutCoverage: Set[String] = withoutCoverage.flatMap(_.source).toSet

  lazy val sourceToCoverage: Map[String, Set[String]] = {
    toMultimap(records.flatMap(r => r.source.flatMap(s => r.coverage.map(c => s -> c))))
  }

  lazy val sourcesByCoverageCount: Map[Int, Set[String]] = toMultimap(allSources.map { source =>
    val coverages = sourceToCoverage.getOrElse(source, Set.empty[String])
    coverages.size -> source
  })

  lazy val allSubjectKeywords: List[String] = {
    bySubject.keys.flatMap(s => s.split("\\s").map(t => t.trim)).toList.distinct.sorted
  }

  // --------------------------------------------------
  // Private

  private def toMultimap[K, V](tuples: List[(K, V)]): Map[K, Set[V]] = {
    tuples.groupBy(_._1).map {
      case (key: K, values: List[(K, V)]) =>
        key -> values.map(_._2).toSet
    }
  }
}

object Records {
  val log = Logger.getLogger(Records.getClass)

  def main(args: Array[String]): Unit = {
    val eothxtf = Paths.get("").resolve("eothxtf")
    log.debug(s"eothxtf directory: $eothxtf")
    val data = eothxtf.resolve("data")
    val eoth08 = data.resolve("eoth08").toFile
    val eoth12 = data.resolve("eoth12").toFile

    val recordsByYear = Stream(eoth08, eoth12).map { d =>
      val xmlFiles = d.listFiles().filter(_.getName.endsWith(".xml"))

      var handleCount = 0
      val records = new Records(xmlFiles.flatMap({ f: File =>
        handleCount = handleCount + 1
        if (handleCount % 100 == 0) {
          //          println(s" $handleCount")
        }
        Record.fromFile(f)
      }).toList)
      d -> records
    }.toMap

    List(
      printSubjects _,
      printSourceAndCoverage _
    ).foreach { f =>
      recordsByYear.foreach { case (d: File, records: Records) =>
        f.apply(d, records)
      }
    }
  }

  def printSourceAndCoverage(directory: File, records: Records): Unit = {
    val dirName = directory.getName
    println("### Mapping source to coverage (" + dirName + ")\n")

    val counts = records.sourcesByCoverageCount.keys.toList.sorted.reverse
    counts.foreach { count =>
      val sourceSet = records.sourcesByCoverageCount.getOrElse(count, Set.empty[String])
      val sources = sourceSet.toList.sorted

      println(s"#### Sources appearing in $count coverage areas for $dirName")
      println()

      if (count != 1) {
        val list = sources.mkString(", ")
        println(s"- $list")
      } else {
        records.allCoverage.foreach { coverage =>
          val list = records.coverageToSource.getOrElse(coverage, Set.empty[String]).filter(sourceSet.contains(_)).toList.sorted.mkString(", ")
          println(s"- $coverage")
          println(s"  - $list")
        }
      }

      println()
    }

  }

  private def printSubjects(directory: File, records: Records): Unit = {
    println("### Longest subject (" + directory.getName + ")")
    println()
    val withLongestSubject = records.withLongestSubject
    println("**File:** [" + withLongestSubject.relativePath + "](https://github.com/CDLUC3/eothxtf/blob/master/data/" + withLongestSubject.relativePath + ")")
    println()
    println("**ID:** `" + withLongestSubject.identifier + "`")
    println()
    println("**Subject (all one entry!):**")
    println()
    println("- " + withLongestSubject.longestSubject)
    println()

    println("#### Most subjects (" + directory.getName + ")")
    println()
    val withMostSubjects = records.withMostSubjects
    println("**File:** [" + withMostSubjects.relativePath + "](https://github.com/CDLUC3/eothxtf/tree/master/data" + withMostSubjects.relativePath + ")")
    println()
    println("**ID:** `" + withMostSubjects.identifier + "`")
    println()
    println("**Subjects:**")
    println("- " + withMostSubjects.subject.mkString(", "))
    println()

    println("#### All subject keywords ("+ directory.getName + ")")
    println()
    println("- " + records.allSubjectKeywords.mkString(", "))
    println()
  }
}
