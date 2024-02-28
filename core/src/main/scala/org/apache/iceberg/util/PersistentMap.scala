package org.apache.iceberg.util

import java.util.Comparator
import java.util.TreeMap
import org.apache.iceberg.relocated.com.google.common.collect.Maps

class MapKey(
  val bytes: Array[Byte],
  val snapSequence: Long) {
}

object MapKey extends Comparator[MapKey] {
  def compare(obj1: MapKey, obj2: MapKey): Int = {
    val inequalPair = obj1.bytes.zip(obj2.bytes).find({case (byte1, byte2) => byte1 != byte2})
    val compReslt = inequalPair.map({
      case (byte1, byte2) =>
        if (byte1 < byte2) {
          -1
        } else {
          1
        }
    })
    compReslt.orElse({
      val cmp = obj1.bytes.lengthCompare(obj2.bytes.length)
      if (cmp == 0) {
        Some(obj1.snapSequence.compare(obj2.snapSequence))
      } else {
        Some(cmp)
      }
    }).get
  }
}

class PersistentMap private {
  private val impl: TreeMap[MapKey, Seq[Byte]] = Maps.newTreeMap(MapKey)

  def getVal(key: MapKey): Array[Byte] = {
    throw new UnsupportedOperationException(classOf[PersistentMap].getName)
  }
}

object PersistentMap {
  val instance: PersistentMap = new PersistentMap
}
