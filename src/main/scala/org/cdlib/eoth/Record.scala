package org.cdlib.eoth

import java.io.File
import java.net.URI
import java.nio.file.{Paths, Path}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{LocalDateTime, LocalDate}

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.xml._

case class Record(
  coverage: Option[String],
  date: List[Either[LocalDate, LocalDateTime]],
  description: Option[String],
  format: String,
  identifier: URI,
  provenance: URI,
  relation: URI,
  source: List[String],
  subject: List[String],
  title: Option[String],
  `type`: String,
  file: Option[File] = None
  ) {

  lazy val hasCoverage = coverage.isDefined
  lazy val hasDescription = description.isDefined
  lazy val hasSubject = subject.nonEmpty
  lazy val hasSource = source.nonEmpty
  lazy val hasTitle = title.isDefined
  lazy val hasFile = file.isDefined

  lazy val numSoruces = source.size
  lazy val numSubjects = subject.size
  lazy val numDates = date.size

  lazy val subjectsByLength = subject.sortBy(s => s.trim.length)
  lazy val longestSubject = subject.lastOption match {
    case Some(s) => s
    case _ => ""
  }

  lazy val absolutePath = file match {
    case Some(f) => f.getAbsolutePath
    case _ => ""
  }

  lazy val relativePath = file match {
    case Some(f) =>
      val path = Paths.get(f.toURI)
      val lastTwo = path.subpath(path.getNameCount - 2, path.getNameCount)
      lastTwo.toString
    case _ => ""
  }
}


object Record {
  val log = Logger.getLogger(Record.getClass)
  val DC_TERMS = List("abstract", "accessRights", "accrualMethod", "accrualPeriodicity", "accrualPolicy", "alternative", "audience", "available", "bibliographicCitation", "conformsTo", "contributor", "coverage", "created", "creator", "date", "dateAccepted", "dateCopyrighted", "dateSubmitted", "description", "educationLevel", "extent", "format", "hasFormat", "hasPart", "hasVersion", "identifier", "instructionalMethod", "isFormatOf", "isPartOf", "isReferencedBy", "isReplacedBy", "isRequiredBy", "isVersionOf", "issued", "language", "license", "mediator", "medium", "modified", "provenance", "publisher", "references", "relation", "replaces", "requires", "rights", "rightsHolder", "source", "spatial", "subject", "tableOfContents", "temporal", "title", "type", "valid")

  val TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  def apply(f: File): Record = {
    val xml = XML.loadFile(f)
    val terms: Map[String, Seq[String]] = DC_TERMS.map({ dc =>
      dc -> (xml \ dc).map(n => n.text.trim).filter(!StringUtils.isBlank(_))
    }).toMap
    Record(
      terms("coverage").headOption,
      terms("date").map(d => parseDate(d)).toList,
      terms("description").headOption,
      terms("format").head,
      new URI(terms("identifier").head).normalize(),
      new URI(terms("provenance").head).normalize(),
      new URI(terms("relation").head).normalize(),
      terms("source").toList,
      terms("subject").toList,
      terms("title").headOption,
      terms("type").head,
      Some(f)
    )
  }

  def parseDate(d: String): Either[LocalDate, LocalDateTime] = {
    try {
      Left(LocalDate.parse(d))
    } catch {
      case e: DateTimeParseException =>
        Right(LocalDateTime.parse(d, TIME_FORMAT))
    }
  }

  def fromFile(f: File): Option[Record] = {
//    print('.')
    try {
      Some(Record(f))
    } catch {
      case e: Exception =>
        log.error(f.getAbsolutePath+" failed: " + e.getMessage)
        None
    }
  }
}
