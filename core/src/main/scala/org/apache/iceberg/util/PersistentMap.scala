package org.apache.iceberg.util

import java.util.Comparator
import java.util.TreeMap
import scala.collection.mutable.Map
import org.apache.iceberg.relocated.com.google.common.collect.Maps

object KeyType extends Enumeration {
  case class Domain(domainId: Int) extends super.Val {}
  import scala.language.implicitConversions
  implicit def valueToKeyTypeVal(x: Value): Domain = x.asInstanceOf[Domain]

  val ByteArray   = Domain(0)
  val CutSequence = Domain(1)
}

class MapKey(
  val domain: KeyType.Domain,
  val bytes: Array[Byte],
  val snapSequence: Long) {
}

object MapKey extends Comparator[MapKey] {
  private val comparators: Map[KeyType.Domain, (MapKey, MapKey) => Int] = Map((KeyType.ByteArray, compareByteArray))

  private def compareByteArray(obj1: MapKey, obj2: MapKey): Int = {
    val inequalPair = obj1.bytes.zip(obj2.bytes).find({case (byte1, byte2) => byte1 != byte2})
    val cmpReslt = inequalPair.map({
      case (byte1, byte2) =>
        if (byte1 < byte2) {
          -1
        } else {
          1
        }
    })
    cmpReslt.orElse({
      val cmp = obj1.bytes.lengthCompare(obj2.bytes.length)
      if (cmp == 0) {
        Some(obj1.snapSequence.compare(obj2.snapSequence))
      } else {
        Some(cmp)
      }
    }).get
  }

  def compare(obj1: MapKey, obj2: MapKey): Int = {
    if (obj1.domain != obj2.domain) {
      obj1.domain.domainId - obj2.domain.domainId
    } else {
      val comparator = comparators(obj1.domain)
      comparator(obj1, obj2)
    }
  }

  def registerComparator(domain: KeyType.Domain, comparator: (MapKey, MapKey) => Int) = {
    comparators(domain) = comparator
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
