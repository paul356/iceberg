package org.apache.iceberg

import java.io.IOException
import java.io.UncheckedIOException
import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.util.{List => JList}
import java.util.{Map => JMap}

import org.apache.iceberg.CutOpType
import org.apache.iceberg.ManifestEntry.Status
import org.apache.iceberg.ManifestReader.FileType
import org.apache.iceberg.ManifestWriter.UNASSIGNED_SEQ
import org.apache.iceberg.avro.Avro
import org.apache.iceberg.io.QDTreeKvdbInputFile
import org.apache.iceberg.io.QDTreeKvdbOutputFile
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.util.KeyType._
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.PersistentMap

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._
import scala.util.Using

class QDTreeManifestEntryWriter[T <: ContentFile[T]] private(
  val metaStore: PersistentMap,
  val snapshotId: JLong,
  val content: ManifestContent,
  createFileWriter: (OutputFile) => ManifestWriter[T],
  createFileReader: (InputFile) => ManifestReader[T]) extends ManifestEntryAppender[T] {
  private val writeBatch = HashMap.empty[MapKey, List[ManifestEntry[T]]]

  private var addedFiles: Int = 0
  private var addedRows: Long = 0
  private var existingFiles: Int = 0
  private var existingRows: Long = 0
  private var deletedFiles: Int = 0
  private var deletedRows: Long = 0
  private var minDataSequenceNumber: JLong = null

  import QDTreeManifestEntryWriter.specialKey

  override def add(datum: T): Unit = {
    // 1. get lower & upper bounds from data file
    // 2. query the existing dataset
    // 3. merge the data file with the existing dataset
    val oldEntries: List[ManifestEntry[T]] = if (writeBatch.contains(specialKey)) {
      writeBatch.get(specialKey).get
    } else {
      val inputFile = new QDTreeKvdbInputFile(specialKey, metaStore)
      if (inputFile.exists) {
        Using.Manager{ use =>
          // Copy old manifest entries
          val reader = use(createFileReader(inputFile))
          val entries = use(reader.liveEntries())
          val entryIter = use(entries.iterator())
          val scalaIter = entryIter.asScala
          val oldSequence = inputFile.getFoundKey.snapSequence
          scalaIter.map(entry => {
            val newEntry = new GenericManifestEntry[T](null.asInstanceOf[org.apache.avro.Schema])
            newEntry.set(0, Integer.valueOf(Status.EXISTING.id()))
            newEntry.setSnapshotId(entry.snapshotId().longValue())
            newEntry.setDataSequenceNumber(if (entry.dataSequenceNumber() == null) oldSequence else entry.dataSequenceNumber().longValue())
            newEntry.set(3, entry.fileSequenceNumber())
            newEntry.set(4, entry.file().copy(true))

            existingFiles += 1
            existingRows += entry.file().recordCount()
            if (entry.dataSequenceNumber() != null &&
              (minDataSequenceNumber == null || minDataSequenceNumber > entry.dataSequenceNumber())) {
              minDataSequenceNumber = entry.dataSequenceNumber()
            }

            newEntry
          }).toList
        }.get
      } else {
        List.empty[ManifestEntry[T]]
      }
    }

    addedFiles += 1
    addedRows += datum.recordCount()

    val newEntry = new GenericManifestEntry[T](null.asInstanceOf[org.apache.avro.Schema])
    writeBatch.put(specialKey, oldEntries :+ newEntry.wrapAppend(JLong.valueOf(snapshotId), datum))
  }

  override def add(datum: T, dataSequenceNumber: Long): Unit = {
    throw new UnsupportedOperationException("unsupported operation")
  }

  override def toManifestFiles: JList[ManifestFile] = {
    List[ManifestFile](new GenericManifestFile(
      QDTreeSnapshot.dataManifestFileKey(snapshotId.longValue()),
      0,
      0,
      content,
      UNASSIGNED_SEQ,
      if (minDataSequenceNumber != null) minDataSequenceNumber.longValue() else UNASSIGNED_SEQ,
      snapshotId,
      addedFiles,
      addedRows,
      existingFiles,
      existingRows,
      deletedFiles,
      deletedRows,
      null,
      null)).asJava
  }

  override def close: Unit = {
  }

  override def commit(sequenceNumber: Long): Unit = {
    val readyBatch = writeBatch.foreach((pair) => {
      val newKey = new MapKey(pair._1.version, pair._1.domain, pair._1.getBytes, sequenceNumber)
      val outputFile = new QDTreeKvdbOutputFile(newKey, metaStore)
      Using(createFileWriter(outputFile)) { writer =>
        pair._2.foreach(writer.addEntry(_))
      }.get
    })
    writeBatch.clear()
  }
}

object QDTreeManifestEntryWriter {
  private def createOldManifestReader[T <: ContentFile[T]](inputFile: InputFile, snapshotId: Long, content: FileType): ManifestReader[T] = {
    new ManifestReader[T](inputFile, 0, null, InheritableMetadataFactory.forCopy(snapshotId), content)
  }

  lazy val specialKey = {
    val simpleCut = new QDTreeCut(0, CutOpType.All, TypeID.BOOLEAN, null, null)
    val byteBuffer = ByteBuffer.allocate(64)
    QDTreeCut.toByteBuffer(byteBuffer, simpleCut)
    byteBuffer.flip()
    new MapKey(domain = CutSequence, byteBuf = byteBuffer, snapSequence = Long.MaxValue)
  }

  def newDataWriter(metaStore: PersistentMap, version: Int, snapshotId: JLong): ManifestEntryAppender[DataFile] = {
    new QDTreeManifestEntryWriter(
      metaStore,
      snapshotId,
      ManifestContent.DATA,
      if (version < 2)
        new ManifestWriter.V1Writer(PartitionSpec.unpartitioned(), _, snapshotId)
      else
        new ManifestWriter.V2Writer(PartitionSpec.unpartitioned(), _, snapshotId),
      createOldManifestReader(_, snapshotId.longValue(), FileType.DATA_FILES))
  }
  def newDeleteWriter(metaStore: PersistentMap, version: Int, snapshotId: JLong): ManifestEntryAppender[DeleteFile] = {
    if (version < 2) {
      throw new IllegalArgumentException("not delete files for version < 2")
    }
    new QDTreeManifestEntryWriter(
      metaStore,
      snapshotId,
      ManifestContent.DELETES,
      new ManifestWriter.V2DeleteWriter(PartitionSpec.unpartitioned(), _, snapshotId),
      createOldManifestReader(_, snapshotId.longValue(), FileType.DELETE_FILES))
  }
}
