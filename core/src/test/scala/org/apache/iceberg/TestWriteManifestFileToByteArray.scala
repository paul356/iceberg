package org.apache.iceberg

import java.nio.ByteBuffer
import java.lang.{Long => JLong}

import org.apache.iceberg.io.ByteArrayOutputFile
import org.apache.iceberg.io.ByteArrayInputFile

import org.junit.Assert._
import org.junit.Test

class TestWriteManifestFileToByteArray {
  @Test
  def testWriteManifestFileToByteArray: Unit = {
    val testManifestFile: ManifestFile = new GenericManifestFile(
      "@ByteArray",
      0L,
      0,
      ManifestContent.DATA,
      1L,
      1L,
      new JLong(0),
      1,
      100L,
      0,
      0L,
      0,
      0L,
      null,
      null)
    val outputFile = new ByteArrayOutputFile

    val writer = ManifestLists.write(2, outputFile, 1, 0, 1)

    writer.add(testManifestFile)
    writer.close()

    val byteArr = outputFile.toByteArray
    print(s"manifest output file length ${byteArr.length}")
    assertTrue(byteArr.length > 0)

    val inputFile = new ByteArrayInputFile(byteArr)
    val manifestFile = ManifestLists.read(inputFile).get(0)

    assertTrue(manifestFile.path() == "@ByteArray")
    assertTrue(manifestFile.addedRowsCount().longValue() == 100L)
  }
}
