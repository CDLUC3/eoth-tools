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

  val DC_ALL = List("abstract", "accessRights", "accrualMethod", "accrualPeriodicity", "accrualPolicy", "alternative", "audience", "available", "bibliographicCitation", "conformsTo", "contributor", "coverage", "created", "creator", "date", "dateAccepted", "dateCopyrighted", "dateSubmitted", "description", "educationLevel", "extent", "format", "hasFormat", "hasPart", "hasVersion", "identifier", "instructionalMethod", "isFormatOf", "isPartOf", "isReferencedBy", "isReplacedBy", "isRequiredBy", "isVersionOf", "issued", "language", "license", "mediator", "medium", "modified", "provenance", "publisher", "references", "relation", "replaces", "requires", "rights", "rightsHolder", "source", "spatial", "subject", "tableOfContents", "temporal", "title", "type", "valid")

  def handle(f: File): Map[String, Seq[String]] = {
    print('.')
    val xml = XML.loadFile(f)
    DC_ALL.map({ dc =>
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
      case (s, m: Map[String, Seq[String]]) =>
        DC_ALL.foreach({ dc: String =>
          val count = m(dc).size
          val counts = allCounts.getOrElse(dc, List[Int]())
          allCounts(dc) = counts :+ count
        })
      case (s, e: Exception) =>
        exceptions(s) = e
    }

    println(s" $handleCount")

    println("------------------------------------------------------------------------------------------------------------------------")

    DC_ALL.foreach({ dc: String =>
      val counts = allCounts.getOrElse(dc, List[Int]())
      val max = counts.max
      if (max > 0) {
        println(s"$dc: $max")
      }
    })

    println("------------------------------------------------------------------------------------------------------------------------")

    exceptions.keys.toList.sorted.foreach({ s =>
      val e = exceptions(s)
      println("%s\t%s".format(s, e.getMessage))
    })
  }
}
