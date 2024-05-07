package org.apache.iceberg

import java.nio.ByteBuffer
import java.lang.{Long => JLong}

import org.apache.iceberg.io.QDTreeKvdbFileIO

import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import org.junit.After

class TestWriteManifestFileToByteArray {
  val writeKey = "manifestfilekey"
  val fileIO = new QDTreeKvdbFileIO(null)

  @Before
  def setUp: Unit = {
    fileIO.deleteFile(writeKey)
  }

  @After
  def tearDown: Unit = {
    fileIO.deleteFile(writeKey)
  }

  @Test
  def testWriteManifestFileToByteArray: Unit = {
    val testManifestFile: ManifestFile = new GenericManifestFile(
      "@ByteArray",
      0L,
      0,
      ManifestContent.DATA,
      1L,
      1L,
      JLong.valueOf(0),
      1,
      100L,
      0,
      0L,
      0,
      0L,
      null,
      null)
    val outputFile = fileIO.newOutputFile(writeKey)

    val writer = ManifestLists.write(2, outputFile, 1, 0, 1)

    writer.add(testManifestFile)
    writer.close()

    val inputFile = fileIO.newInputFile(writeKey)
    val manifestFile = ManifestLists.read(inputFile).get(0)

    assertTrue(manifestFile.path() == "@ByteArray")
    assertTrue(manifestFile.addedRowsCount().longValue() == 100L)
  }
}
