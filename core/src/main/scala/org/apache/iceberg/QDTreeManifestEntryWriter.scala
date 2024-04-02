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
import org.apache.iceberg.avro.Avro
import org.apache.iceberg.io.ByteBufferInputFile
import org.apache.iceberg.io.ByteBufferOutputFile
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
  val formatVersion: Int,
  val snapshotId: JLong,
  createFileWriter: (OutputFile) => ManifestWriter[T],
  createFileReader: (InputFile) => ManifestReader[T]) extends ManifestEntryAppender[T] {
  private val writeBatch = HashMap.empty[MapKey, List[ManifestEntry[T]]]

  import QDTreeManifestEntryWriter.specialKey

  private def getExistingManifest: Option[(MapKey, ByteBuffer)] = {
    val currentEntry = metaStore.getValWithSequenceFallback(specialKey)
    if (currentEntry != null)
      Some(currentEntry)
    else
      None
  }

  override def add(datum: T): Unit = {
    // 1. get lower & upper bounds from data file
    // 2. query the existing dataset
    // 3. merge the data file with the existing dataset
    val oldEntries: List[ManifestEntry[T]] = if (writeBatch.contains(specialKey)) {
      writeBatch.get(specialKey).get
    } else {
      val existingManifest = getExistingManifest
      if (!existingManifest.isEmpty) {
        val byteBuffer = existingManifest.get._2
        val oldSequence = existingManifest.get._1.snapSequence
        Using.Manager{ use =>
          val inputFile = new ByteBufferInputFile(List(byteBuffer).asJava)
          // Copy old manifest entries
          val reader = use(createFileReader(inputFile))
          val entries = use(reader.liveEntries())
          val entryIter = use(entries.iterator())
          // Should I use existing()?
          val scalaIter = entryIter.asScala
          scalaIter.map(entry => {
            val newEntry = new GenericManifestEntry[T](null.asInstanceOf[org.apache.avro.Schema])
            newEntry.set(0, Integer.valueOf(Status.EXISTING.id()))
            newEntry.setSnapshotId(entry.snapshotId().longValue())
            newEntry.setDataSequenceNumber(if (entry.dataSequenceNumber() == null) oldSequence else entry.dataSequenceNumber().longValue())
            newEntry.set(3, entry.fileSequenceNumber())
            newEntry.set(4, entry.file().copy(true))

            newEntry
          }).toList
        }.get
      } else {
        List.empty[ManifestEntry[T]]
      }
    }
    val newEntry = new GenericManifestEntry[T](null.asInstanceOf[org.apache.avro.Schema])
    writeBatch.put(specialKey, oldEntries :+ newEntry.wrapAppend(JLong.valueOf(snapshotId), datum))
  }

  override def add(datum: T, dataSequenceNumber: Long): Unit = {
    throw new UnsupportedOperationException("unsupported operation")
  }

  override def toManifestFiles: JList[ManifestFile] = {
    null
  }

  override def close: Unit = {
  }

  override def commit(sequenceNumber: Long): Unit = {
    val readyBatch = writeBatch.map((pair) => {
      val outputFile = new ByteBufferOutputFile
      Using(createFileWriter(outputFile)) { writer =>
        pair._2.foreach(writer.addEntry(_))
      }.get
      val newKey = new MapKey(pair._1.version, pair._1.domain, pair._1.getBytes, sequenceNumber)
      val bufs = outputFile.toByteBuffer
      if (bufs.size() == 1) {
        (newKey, bufs.get(0))
      } else {
        val totalLen = bufs.asScala.foldLeft(0)((sum, entry) => {
          sum + entry.remaining()
        })
        val combineBuf = ByteBuffer.allocateDirect(totalLen)
        bufs.asScala.foreach((entry) => {
          val arr = Array.ofDim[Byte](entry.remaining())
          entry.get(arr)
          combineBuf.put(arr)
        })
        (newKey, combineBuf)
      }
    })
    metaStore.putBatch(readyBatch.asJava)
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
      metaStore,version,
      snapshotId,
      new ManifestWriter.V2Writer(PartitionSpec.unpartitioned(), _, snapshotId),
      createOldManifestReader(_, snapshotId.longValue(), FileType.DATA_FILES))
  }
  def newDeleteWriter(metaStore: PersistentMap, version: Int, snapshotId: JLong): ManifestEntryAppender[DeleteFile] = {
    new QDTreeManifestEntryWriter(
      metaStore,
      version,
      snapshotId,
      new ManifestWriter.V2DeleteWriter(PartitionSpec.unpartitioned(), _, snapshotId),
      createOldManifestReader(_, snapshotId.longValue(), FileType.DELETE_FILES))
  }
}
