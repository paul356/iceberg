package org.apache.iceberg.io

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{List => JList}

import org.apache.avro.util.ByteBufferOutputStream
import org.apache.iceberg.exceptions.AlreadyExistsException
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.PersistentMap
import scala.jdk.CollectionConverters._

class QDTreeKvdbOutputFile(path: MapKey, metaStore: PersistentMap) extends OutputFile {
  class ByteBufferPositionOutputStream extends PositionOutputStream {
    private var offset: Long = 0
    private var closed = false
    private val byteBufferStream = new ByteBufferOutputStream

    override def getPos: Long = offset

    override def close: Unit = {
      if (closed) {
        return
      }

      closed = true
      byteBufferStream.close()

      val bufs = byteBufferStream.getBufferList()
      if (bufs.size() == 1) {
        metaStore.putVal(path, bufs.get(0))
      } else {
        val totalLen = bufs.asScala.foldLeft(0)((sum, entry) => {
          sum + entry.remaining()
        })
        val combinedBuf = ByteBuffer.allocateDirect(totalLen)
        bufs.asScala.foreach((entry) => {
          val arr = Array.ofDim[Byte](entry.remaining())
          entry.get(arr)
          combinedBuf.put(arr)
        })
        metaStore.putVal(path, combinedBuf)
      }
    }

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

  override lazy val location: String = {
    QDTreeKvdbUtils.getPathFromKey(path)
  }

  override def toInputFile: InputFile = {
    if (positionStream != null) {
      positionStream.close()
      new QDTreeKvdbInputFile(path, metaStore)
    } else {
      return null
    }
  }
}
