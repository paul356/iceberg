package org.apache.iceberg.io

import java.io.FileNotFoundException
import java.nio.ByteBuffer

import org.apache.iceberg.io.ByteBufferInputStream
import org.apache.iceberg.util.KeyType
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.PersistentMap
import scala.jdk.CollectionConverters._

class QDTreeKvdbInputFile(path: MapKey, @transient metaStore: PersistentMap) extends InputFile {
  private lazy val kvValue = {
    val returnKey = metaStore.getValWithSequenceFallback(path)
    if (returnKey == null) {
      throw new FileNotFoundException(location)
    }

    returnKey
  }

  def getFoundKey: MapKey = kvValue._1

  override def getLength: Long = kvValue._2.remaining()

  override def newStream: SeekableInputStream = {
    ByteBufferInputStream.wrap(List(kvValue._2).asJava)
  }

  override lazy val location: String = {
    QDTreeKvdbUtils.getPathFromKey(path)
  }

  override def exists: Boolean = {
    try {
      kvValue != null
    } catch {
      case e: FileNotFoundException => false
    }
  }
}
