package org.apache.iceberg.io

import org.junit.Assert._
import org.junit.Test

class TestByteArrayInputOutput {
  @Test
  def testByteArrayOutput: Unit = {
    val outputFile = new ByteArrayOutputFile
    val positionOutputStream = outputFile.create
    positionOutputStream.write(Array[Byte]('1', '2', '3'))
    positionOutputStream.write('a')
    positionOutputStream.write('b')
    positionOutputStream.write('c')
    positionOutputStream.write(Array[Byte]('1', '2', '3', '4', '5', '6'), 3, 3)
    val str = positionOutputStream.asInstanceOf[outputFile.ByteArrayPositionOutputStream].toByteArray.map(_.toChar).mkString
    print(str)
    assertTrue(str == "123abc456")
  }

  @Test
  def testByteArrayInput: Unit = {
    val byteArr = Array[Byte]('1', '2', '3', 'a', 'b', 'c', '4', '5', '6')
    val inputFile = new ByteArrayInputFile(byteArr)
    val inputStream = inputFile.newStream

    assertTrue(inputStream.available == byteArr.length)
    assertTrue(inputStream.markSupported == true)

    val readArr = Array.ofDim[Byte](3)
    inputStream.read(readArr)

    assertTrue(byteArr.slice(0, 3).sameElements(readArr))
    assertTrue(inputStream.available == 6)

    val readArr2 = Array.ofDim[Byte](9)
    inputStream.read(readArr2, 3, 6)

    assertTrue(byteArr.slice(3, 9).sameElements(readArr2.slice(3, 9)))

    inputStream.reset
    inputStream.read(readArr2)

    assertTrue(byteArr.sameElements(readArr2))
  }
}
