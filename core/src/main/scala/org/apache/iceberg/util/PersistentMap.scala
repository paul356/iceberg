package org.apache.iceberg.util

import java.nio.ByteBuffer
import java.util.Comparator
import java.util.TreeMap
import org.apache.iceberg.relocated.com.google.common.collect.Maps
import scala.collection.mutable.Map

object KeyType extends Enumeration {
  case class Domain(domainId: Int) extends super.Val {}
  import scala.language.implicitConversions
  implicit def valueToKeyTypeVal(x: Value): Domain = x.asInstanceOf[Domain]

  val ByteArray   = Domain(0)
  val CutSequence = Domain(1)
}

class MapKey(
  val version: Int = 1,
  val domain: KeyType.Domain,
  byteBuf: ByteBuffer,
  val snapSequence: Long) {
  private val bytes = byteBuf.duplicate()
  bytes.rewind()
  def getBytes: ByteBuffer = {
    val res = bytes.duplicate()
    res.rewind()
    res
  }
}

object MapKey extends Comparator[MapKey] {
  private val comparators: Map[KeyType.Domain, (MapKey, MapKey) => Int] = Map((KeyType.ByteArray, compareByteArray))
  private def compareByteArray(obj1: MapKey, obj2: MapKey): Int = {
    val cmp = obj1.bytes.compareTo(obj2.bytes)
    if (cmp == 0) {
      obj1.snapSequence.compare(obj2.snapSequence)
    } else {
      cmp
    }
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
  private val impl: TreeMap[MapKey, ByteBuffer] = Maps.newTreeMap(MapKey)

  def getVal(key: MapKey): ByteBuffer = {
    impl.get(key)
  }

  def putVal(key: MapKey, value: ByteBuffer): Unit = {
    impl.put(key, value)
  }
}

object PersistentMap {
  val instance: PersistentMap = new PersistentMap
}
