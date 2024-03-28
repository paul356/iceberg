package org.apache.iceberg.io

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{List => JList}

import org.apache.avro.util.ByteBufferInputStream
import scala.jdk.CollectionConverters._

class ByteBufferInputFile(byteBufs: JList[ByteBuffer]) extends InputFile {
  class ByteBufferSeekableInputStream extends SeekableInputStream {
    private var offset: Long = 0
    private val byteBufferStream = new ByteBufferInputStream(byteBufs)

    override def getPos: Long = offset

    override def seek(newPos: Long) = {
      byteBufferStream.reset()
      byteBufferStream.skip(newPos)
      offset = newPos
    }

    override def read: Int = {
      byteBufferStream.read
    }

    override def read(b: Array[Byte]): Int = {
      byteBufferStream.read(b)
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      byteBufferStream.read(b, off, len)
    }

    override def skip(n: Long): Long = byteBufferStream.skip(n)

    override def available: Int = byteBufferStream.available()

    override def close = byteBufferStream.close()

    override def mark(readLimit: Int) = byteBufferStream.mark(readLimit)

    override def reset = byteBufferStream.reset

    override def markSupported: Boolean = byteBufferStream.markSupported
  }

  override val getLength: Long = byteBufs.asScala.foldLeft(0)((a, b) => a + b.remaining())

  override def newStream: SeekableInputStream = {
    new ByteBufferSeekableInputStream
  }

  override def location: String = {
    "@ByteAraryInputStream"
  }

  override def exists: Boolean = {
    true
  }
}
