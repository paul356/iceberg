package org.apache.iceberg

import java.util.{List => JList}

import org.apache.iceberg.util.MapKey
import scala.collection.mutable.HashMap

class QDTreeManifestEntryWriter[FileType <: ContentFile[FileType]] private extends ManifestEntryAppender[FileType] {
  private val writeBatch: HashMap[MapKey, Array[Byte]] = HashMap.empty[MapKey, Array[Byte]]
  def add(datum: FileType): Unit = {
    // 1. get lower & upper bounds from data file
    // 2. query the existing dataset
    // 3. merge the data file with the existing dataset
  }

  def add(datum: FileType, dataSequenceNumber: Long): Unit = {
  }

  def toManifestFiles: JList[ManifestFile] = {
    null
  }

  def close: Unit = {
  }
}


object QDTreeManifestEntryWriter {
  def apply[FileType <: ContentFile[FileType]](formatVersion: Int): QDTreeManifestEntryWriter[FileType] = {
    assert(formatVersion > 1)
    new QDTreeManifestEntryWriter
  }
}
