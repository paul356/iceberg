package org.apache.iceberg

import java.io.IOException
import java.io.UncheckedIOException
import java.lang.{Long => JLong}
import java.util.{List => JList}
import java.util.{Map => JMap}

import org.apache.iceberg.avro.Avro
import org.apache.iceberg.io.ByteArrayOutputFile
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.util.MapKey

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

class QDTreeManifestEntryWriter[FileType <: ContentFile[FileType]] private(
  val formatVersion: Int,
  val snapshotId: JLong,
  val extraAttrs: JMap[String, String]) extends ManifestEntryAppender[FileType] {
  private val writeBatch: HashMap[MapKey, Array[Byte]] = HashMap.empty[MapKey, Array[Byte]]
  def add(datum: FileType): Unit = {
    // 1. get lower & upper bounds from data file
    // 2. query the existing dataset
    // 3. merge the data file with the existing dataset
    val outBufFile = new ByteArrayOutputFile
    val appender = newAppender(outBufFile)

    
  }

  def add(datum: FileType, dataSequenceNumber: Long): Unit = {
  }

  def toManifestFiles: JList[ManifestFile] = {
    null
  }

  def close: Unit = {
  }

  def newAppender(file: OutputFile): FileAppender[QDTreeManifestEntry[DataFile]] = {
    val schema = QDTreeManifestEntry.getSchema
    try {
      Avro.write(file)
        .schema(schema)
        .named("manifest_entry")
        .meta("format-version", "2")
        .meta(extraAttrs)
        .overwrite()
        .build()
    } catch {
      case e: IOException => throw new UncheckedIOException("fail to create QDTree ManifestEntry writer", e)
    }
  }
}


object QDTreeManifestEntryWriter {
  def newDataWriter(version: Int, snapshotId: JLong): ManifestEntryAppender[DataFile] = {
    val extraAttrs = Map(("content", "data"))
    new QDTreeManifestEntryWriter(version, snapshotId, extraAttrs.asJava)
  }
  def newDeleteWriter(version: Int, snapshotId: JLong): ManifestEntryAppender[DeleteFile] = {
    val extraAttrs = Map(("content", "deletes"))
    new QDTreeManifestEntryWriter(version, snapshotId, extraAttrs.asJava)
  }
}
