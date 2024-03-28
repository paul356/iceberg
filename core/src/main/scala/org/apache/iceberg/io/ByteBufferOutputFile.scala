package org.apache.iceberg.io

import java.io.ByteArrayOutputStream
import java.io.IOException

import org.apache.iceberg.exceptions.AlreadyExistsException

class ByteArrayOutputFile extends OutputFile {
  class ByteArrayPositionOutputStream extends PositionOutputStream {
    private var offset: Long = 0
    private val byteArrayStream = new ByteArrayOutputStream

    override def getPos: Long = offset

    override def close = byteArrayStream.close()

    override def flush = byteArrayStream.flush()

    override def write(b: Array[Byte]) = {
      byteArrayStream.write(b)
      offset += b.length
    }

    override def write(b: Array[Byte], off: Int, len: Int) = {
      byteArrayStream.write(b, off, len)
      offset += len
    }

    override def write(b: Int) = {
      byteArrayStream.write(b)
      offset += 1
    }

    def toByteArray: Array[Byte] = byteArrayStream.toByteArray
  }

  private var positionStream: ByteArrayPositionOutputStream = null

  @throws(classOf[AlreadyExistsException])
  override def create: PositionOutputStream = {
    if (positionStream != null) {
      throw new AlreadyExistsException("An output stream already exists")
    }
    positionStream = new ByteArrayPositionOutputStream
    positionStream
  }

  override def createOrOverwrite: PositionOutputStream = {
    create
  }

  override def location: String = {
    "@ByteArrayOutputStream"
  }

  override def toInputFile: InputFile = {
    val arr = positionStream.toByteArray
    new ByteArrayInputFile(arr)
  }

  def toByteArray: Array[Byte] = positionStream.toByteArray
}
