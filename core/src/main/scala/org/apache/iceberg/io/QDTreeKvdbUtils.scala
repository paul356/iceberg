package org.apache.iceberg.io

import java.nio.ByteBuffer

import org.apache.iceberg.util.KeyType
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.PersistentMap

object QDTreeKvdbUtils {
  def getKeyFromPath(path: String): MapKey = {
    new MapKey(domain = KeyType.ByteArray, byteBuf = ByteBuffer.wrap(path.getBytes("ISO-8859-1")), snapSequence = 0)
  }

  def getPathFromKey(path: MapKey): String = {
    if (path.domain == KeyType.ByteArray) {
      val bytes = path.getBytes
      val arr = Array.ofDim[Byte](bytes.remaining())
      bytes.get(arr)
      arr.map(_.toChar).toSeq.mkString
    } else {
      "file://nonbytearray.avro"
    }
  }
}
