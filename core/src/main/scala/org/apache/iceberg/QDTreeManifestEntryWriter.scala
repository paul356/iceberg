package org.apache.iceberg

import java.io.IOException
import java.io.UncheckedIOException
import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.util.{List => JList}
import java.util.{Map => JMap}

import org.apache.iceberg.CutOpType
import org.apache.iceberg.avro.Avro
import org.apache.iceberg.io.ByteBufferOutputFile
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.util.KeyType._
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.PersistentMap

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

class QDTreeManifestEntryWriter[FileType <: ContentFile[FileType]] private(
  val metaStore: PersistentMap,
  val formatVersion: Int,
  val snapshotId: JLong,
  val createFileWriter: (OutputFile) => ManifestWriter[FileType]) extends ManifestEntryAppender[FileType] {
  private val writeBatch: HashMap[MapKey, Array[Byte]] = HashMap.empty[MapKey, Array[Byte]]

  private def getExistingEntries: Unit = {
    val simpleCut = new QDTreeCut(0, CutOpType.All, TypeID.BOOLEAN, null, null)
    val byteBuffer = ByteBuffer.allocate(64)
    QDTreeCut.toByteBuffer(byteBuffer, simpleCut)
    val mapKey = new MapKey(domain = CutSequence, byteBuf = byteBuffer.flip(), snapSequence = 0)

    metaStore.getVal(mapKey)
  }

  def add(datum: FileType): Unit = {
    // 1. get lower & upper bounds from data file
    // 2. query the existing dataset
    // 3. merge the data file with the existing dataset
    val outBufFile = new ByteBufferOutputFile
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
  def newDataWriter(metaStore: PersistentMap, version: Int, snapshotId: JLong): ManifestEntryAppender[DataFile] = {
    new QDTreeManifestEntryWriter(metaStore, version, snapshotId, new ManifestWriter.V2Writer(PartitionSpec.unpartitioned(), _, snapshotId))
  }
  def newDeleteWriter(metaStore: PersistentMap, version: Int, snapshotId: JLong): ManifestEntryAppender[DeleteFile] = {
    new QDTreeManifestEntryWriter(metaStore, version, snapshotId, new ManifestWriter.V2DeleteWriter(PartitionSpec.unpartitioned(), _, snapshotId))
  }
}
