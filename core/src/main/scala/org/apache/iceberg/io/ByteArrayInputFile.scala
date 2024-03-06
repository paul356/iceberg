package org.apache.iceberg.io

import java.io.ByteArrayInputStream
import java.io.IOException

class ByteArrayInputFile(byteArr: Array[Byte]) extends InputFile {
  class ByteArraySeekableInputStream(
    byteArr: Array[Byte]
  ) extends SeekableInputStream {
    private var offset: Long = 0
    private val byteArrayStream = new ByteArrayInputStream(byteArr)

    override def getPos: Long = offset

    override def seek(newPos: Long) = {
      byteArrayStream.reset()
      byteArrayStream.skip(newPos)
      offset = newPos
    }

    override def read: Int = {
      byteArrayStream.read
    }

    override def read(b: Array[Byte]): Int = {
      byteArrayStream.read(b)
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      byteArrayStream.read(b, off, len)
    }

    override def skip(n: Long): Long = byteArrayStream.skip(n)

    override def available: Int = byteArrayStream.available()

    override def close = byteArrayStream.close()

    override def mark(readLimit: Int) = byteArrayStream.mark(readLimit)

    override def reset = byteArrayStream.reset

    override def markSupported: Boolean = byteArrayStream.markSupported
  }

  override def getLength: Long = byteArr.length

  override def newStream: SeekableInputStream = {
    new ByteArraySeekableInputStream(byteArr)
  }

  override def location: String = {
    "@ByteAraryInputStream"
  }

  override def exists: Boolean = {
    true
  }
}
