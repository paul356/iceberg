package org.apache.iceberg

import java.lang.{Long => JLong}

import org.apache.iceberg.util.PersistentMap

import org.junit.Assert._
import org.junit.Test

class TestQDTreeManifestEntryWriter {
  @Test
  def testWriteSimpleManifestEntry: Unit = {
    val metaStore = PersistentMap.instance
    val snapshot = JLong.valueOf(1)

    val writer = QDTreeManifestEntryWriter.newDataWriter(metaStore, 0x1234, snapshot)
    val dataFile = new GenericDataFile(
      0,
      "file:///first-file",
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
}
