package org.apache.iceberg

import java.util.{List => JList}

class QDTreeManifestEntryWriter[FileType <: ContentFile[FileType]](
  val dataSequenceNumber: Long) extends ManifestEntryAppender[FileType] {
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
  def apply[FileType <: ContentFile[FileType]](sequenceNumber: Long): QDTreeManifestEntryWriter[FileType] = {
    new QDTreeManifestEntryWriter(sequenceNumber)
  }
}
