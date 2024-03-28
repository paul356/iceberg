package org.apache.iceberg.io

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{List => JList}

import org.apache.avro.util.ByteBufferOutputStream
import org.apache.iceberg.exceptions.AlreadyExistsException

class ByteBufferOutputFile extends OutputFile {
  class ByteBufferPositionOutputStream extends PositionOutputStream {
    private var offset: Long = 0
    private val byteBufferStream = new ByteBufferOutputStream

    override def getPos: Long = offset

    override def close = byteBufferStream.close()

    override def flush = byteBufferStream.flush()

    override def write(b: Array[Byte]) = {
      byteBufferStream.write(b)
      offset += b.length
    }

    override def write(b: Array[Byte], off: Int, len: Int) = {
      byteBufferStream.write(b, off, len)
      offset += len
    }

    override def write(b: Int) = {
      byteBufferStream.write(b)
      offset += 1
    }

    def toByteBuffer: JList[ByteBuffer] = byteBufferStream.getBufferList()
  }

  private var positionStream: ByteBufferPositionOutputStream = null

  @throws(classOf[AlreadyExistsException])
  override def create: PositionOutputStream = {
    if (positionStream != null) {
      throw new AlreadyExistsException("An output stream already exists")
    }
    positionStream = new ByteBufferPositionOutputStream
    positionStream
  }

  override def createOrOverwrite: PositionOutputStream = {
    positionStream = new ByteBufferPositionOutputStream
    positionStream
  }

  override def location: String = {
    "@ByteBufferOutputStream"
  }

  override def toInputFile: InputFile = {
    val arr = positionStream.toByteBuffer
    new ByteBufferInputFile(arr)
  }

  def toByteBuffer: JList[ByteBuffer] = positionStream.toByteBuffer
}
