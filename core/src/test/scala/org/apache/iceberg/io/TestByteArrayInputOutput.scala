package org.apache.iceberg.io

import java.nio.ByteBuffer

import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._

class TestByteArrayInputOutput {
  @Test
  def testByteArrayOutput: Unit = {
    val outputFile = new ByteBufferOutputFile
    val positionOutputStream = outputFile.create
    positionOutputStream.write(Array[Byte]('1', '2', '3'))
    positionOutputStream.write('a')
    positionOutputStream.write('b')
    positionOutputStream.write('c')
    positionOutputStream.write(Array[Byte]('1', '2', '3', '4', '5', '6'), 3, 3)
    val bufList = positionOutputStream.asInstanceOf[outputFile.ByteBufferPositionOutputStream].toByteBuffer.asScala
    val str = bufList.foldLeft(List.empty[Char])((a, b) => {
      val dup = b.duplicate()
      var lst = List.empty[Char]
      while (b.remaining() > 0) {
        lst = lst :+ b.get().toChar
      }
      a ++ lst
    }).mkString
    print(str)
    assertTrue(str == "123abc456")
  }

  @Test
  def testByteArrayInput: Unit = {
    val byteArr = Array[Byte]('1', '2', '3', 'a', 'b', 'c', '4', '5', '6')
    val inputFile = new ByteBufferInputFile(List(ByteBuffer.wrap(byteArr)).asJava)
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

    inputStream.seek(0)
    inputStream.read(readArr2)

    assertTrue(byteArr.sameElements(readArr2))
  }
}
