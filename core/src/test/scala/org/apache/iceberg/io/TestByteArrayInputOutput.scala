package org.apache.iceberg.io

import java.nio.ByteBuffer

import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import org.junit.After

import scala.jdk.CollectionConverters._

class TestByteArrayInputOutput {
  val writeKey = "bytearrayiokey"

  @Before
  def setUp: Unit = {
    QDTreeKvdbFileIO.deleteFile(writeKey)
  }

  @After
  def tearDown: Unit = {
    QDTreeKvdbFileIO.deleteFile(writeKey)
  }

  @Test
  def testByteArrayOutput: Unit = {
    val outputFile = QDTreeKvdbFileIO.newOutputFile(writeKey)
    val positionOutputStream = outputFile.create
    positionOutputStream.write(Array[Byte]('1', '2', '3'))
    positionOutputStream.write('a')
    positionOutputStream.write('b')
    positionOutputStream.write('c')
    positionOutputStream.write(Array[Byte]('1', '2', '3', '4', '5', '6'), 3, 3)

    positionOutputStream.close()

    val inputFile = QDTreeKvdbFileIO.newInputFile(writeKey)
    val inputStream = inputFile.newStream

    val byteArr = Array[Byte]('1', '2', '3', 'a', 'b', 'c', '4', '5', '6')
    assertTrue(inputStream.available == byteArr.length)
    assertTrue(inputStream.markSupported == true)

    val readArr = Array.ofDim[Byte](3)
    inputStream.read(readArr)

    assertTrue(byteArr.slice(0, 3).sameElements(readArr))
    assertTrue(inputStream.available == 6)

    val readArr2 = Array.ofDim[Byte](9)
    inputStream.read(readArr2, 3, 6)

    assertTrue(byteArr.slice(3, 9).sameElements(readArr2.slice(3, 9)))

    inputStream.seek(0)
    inputStream.read(readArr2)

    assertTrue(byteArr.sameElements(readArr2))
    inputStream.close()
  }
}
