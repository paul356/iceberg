package org.apache.iceberg

import java.nio.ByteBuffer
import java.lang.{Long => JLong}

import org.apache.iceberg.io.ByteBufferOutputFile
import org.apache.iceberg.io.ByteBufferInputFile

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
      JLong.valueOf(0),
      1,
      100L,
      0,
      0L,
      0,
      0L,
      null,
      null)
    val outputFile = new ByteBufferOutputFile

    val writer = ManifestLists.write(2, outputFile, 1, 0, 1)

    writer.add(testManifestFile)
    writer.close()

    val byteArr = outputFile.toByteBuffer
    print(s"manifest output file length ${byteArr.get(0).remaining()}")
    assertTrue(byteArr.get(0).remaining() > 0)

    val inputFile = new ByteBufferInputFile(byteArr)
    val manifestFile = ManifestLists.read(inputFile).get(0)

    assertTrue(manifestFile.path() == "@ByteArray")
    assertTrue(manifestFile.addedRowsCount().longValue() == 100L)
  }
}
