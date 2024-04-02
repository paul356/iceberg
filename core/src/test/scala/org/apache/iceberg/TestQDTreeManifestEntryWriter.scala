package org.apache.iceberg

import java.lang.{Long => JLong}

import org.apache.iceberg.ManifestReader.FileType
import org.apache.iceberg.io.ByteBufferInputFile
import org.apache.iceberg.util.PersistentMap

import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._
import scala.util.Using

class TestQDTreeManifestEntryWriter {
  private def clearOldKey: Unit = {
    while (true) {
      val pair = PersistentMap.instance.getValWithSequenceFallback(QDTreeManifestEntryWriter.specialKey)
      if (pair != null) {
        PersistentMap.instance.delete(pair._1)
      } else {
        return
      }
    }
  }

  private def getAllSet: List[ManifestEntry[DataFile]] = {
    val pair = PersistentMap.instance.getValWithSequenceFallback(QDTreeManifestEntryWriter.specialKey)
    if (pair == null) {
      return null
    }

    val buf = pair._2.duplicate()
    buf.rewind()
    val inputFile = new ByteBufferInputFile(List(buf).asJava)
    Using.Manager { use =>
      val reader = use(new ManifestReader[DataFile](inputFile, 0, null, InheritableMetadataFactory.empty(), FileType.DATA_FILES))
      val entryIterable = use(reader.liveEntries())
      val entryIter = use(entryIterable.iterator())
      entryIter.asScala.foldLeft(List.empty[ManifestEntry[DataFile]])((lst, entry) => {
        lst :+ entry.copy()
      })
    }.get
  }

  @Test
  def testWriteSimpleManifestEntry: Unit = {
    val metaStore = PersistentMap.instance
    val snapshot = JLong.valueOf(1)

    clearOldKey

    val writer = QDTreeManifestEntryWriter.newDataWriter(metaStore, 2, snapshot)
    val dataFile = new GenericDataFile(
      0,
      "file://first-file",
      FileFormat.PARQUET,
      null,
      0,
      new Metrics(1, null, null, null, null),
      null,
      null,
      null,
      null)
    writer.add(dataFile)
    writer.commit(1)
  }

  @Test
  def testMergeManifestEntry: Unit = {
    val metaStore = PersistentMap.instance
    val snapshot = JLong.valueOf(1)

    clearOldKey

    val writer1 = QDTreeManifestEntryWriter.newDataWriter(metaStore, 1, snapshot)
    val dataFile1 = new GenericDataFile(
      0,
      "file://first-file",
      FileFormat.PARQUET,
      null,
      0,
      new Metrics(1, null, null, null, null),
      null,
      null,
      null,
      null)
    writer1.add(dataFile1)
    val dataFile2 = new GenericDataFile(
      0,
      "file://second-file",
      FileFormat.PARQUET,
      null,
      0,
      new Metrics(2, null, null, null, null),
      null,
      null,
      null,
      null)
    writer1.add(dataFile2)
    writer1.commit(1)

    val snapshot2 = JLong.valueOf(2)
    val writer2 = QDTreeManifestEntryWriter.newDataWriter(metaStore, 2, snapshot2)
    val dataFile3 = new GenericDataFile(
      0,
      "file://third-file",
      FileFormat.PARQUET,
      null,
      0,
      new Metrics(3, null, null, null, null),
      null,
      null,
      null,
      null)
    writer2.add(dataFile3)
    writer2.commit(2)

    val entries = getAllSet
    assertTrue(entries.length == 3)
    var pathSet = Set("file://first-file", "file://second-file", "file://third-file")
    entries.foreach(entry => {
      assertTrue(pathSet.contains(entry.file().path().toString()))
      pathSet = pathSet - entry.file().path().toString()
    })
    assertTrue(pathSet.isEmpty)
  }
}
