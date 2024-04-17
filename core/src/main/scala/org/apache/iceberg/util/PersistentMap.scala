package org.apache.iceberg.util

import java.nio.ByteBuffer
import java.util.Comparator
import java.util.{Map => JMap}
import java.util.TreeMap
import org.apache.iceberg.relocated.com.google.common.collect.Maps
import scala.collection.mutable.Map

import scala.jdk.CollectionConverters._

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
  val snapSequence: Long) extends Ordered[MapKey] {
  private val bytes = byteBuf.duplicate()
  bytes.rewind()
  def getBytes: ByteBuffer = {
    val res = bytes.duplicate()
    res.rewind()
    res
  }

  def compare(that: MapKey): Int = {
    if (this eq that) {
      return 0
    }

    MapKey.compareKeys(this, that)
  }

  override def equals(that: Any): Boolean = {
    if (that == null || !that.isInstanceOf[MapKey]) {
      return false
    }

    compare(that.asInstanceOf[MapKey]) == 0
  }
}

object MapKey {
  private val comparators: Map[KeyType.Domain, (MapKey, MapKey, Boolean) => Int] = Map((KeyType.ByteArray, compareByteArray))
  private def compareByteArray(obj1: MapKey, obj2: MapKey, withSequence: Boolean): Int = {
    val cmp = obj1.bytes.compareTo(obj2.bytes)
    if (cmp == 0 && withSequence) {
      obj1.snapSequence.compare(obj2.snapSequence)
    } else {
      cmp
    }
  }

  def compareKeys(obj1: MapKey, obj2: MapKey): Int = {
    if (obj1.domain != obj2.domain) {
      obj1.domain.domainId - obj2.domain.domainId
    } else {
      val comparator = comparators(obj1.domain)
      comparator(obj1, obj2, true)
    }
  }

  def equalWithoutSequence(obj1: MapKey, obj2: MapKey): Boolean = {
    if (obj1.domain != obj2.domain) {
      false
    } else {
      val comparator = comparators(obj1.domain)
      comparator(obj1, obj2, false) == 0
    }
  }

  def registerComparator(domain: KeyType.Domain, comparator: (MapKey, MapKey, Boolean) => Int) = {
    comparators(domain) = comparator
  }
}

class PersistentMap private {
  private val impl: TreeMap[MapKey, ByteBuffer] = Maps.newTreeMap()

  def getVal(key: MapKey): ByteBuffer = {
    impl.get(key)
  }

  def getValWithSequenceFallback(key: MapKey): (MapKey, ByteBuffer) = {
    val entry = impl.floorEntry(key)
    if (entry != null) {
      if (MapKey.equalWithoutSequence(entry.getKey(), key)) {
        (entry.getKey(), entry.getValue())
      } else {
        null
      }
    } else {
      null
    }
  }

  def putVal(key: MapKey, value: ByteBuffer): Unit = {
    impl.put(key, value)
  }

  def putBatch(writeBatch: JMap[MapKey, ByteBuffer]): Unit = {
    impl.putAll(writeBatch)
  }

  def delete(key: MapKey): Unit = {
    impl.remove(key)
  }

  // key should be in the range of [startKey, endKey), and sequence is smaller than or equal to maxSequence
  def iterator(startKey: Option[MapKey], endKey: Option[MapKey], maxSequence: Option[Long]): Iterator[MapKey] = {
    new Iterator[MapKey] {
      private var currKey = if (startKey.isEmpty) {
        impl.firstKey()
      } else {
        impl.ceilingKey(startKey.get)
      }

      private def inRange(key: MapKey): Boolean = {
        val checkEnd = if (!endKey.isEmpty) {
          key < endKey.get
        } else {
          true
        }

        val checkSequence = if (!maxSequence.isEmpty) {
          key.snapSequence < maxSequence.get || key.snapSequence == maxSequence.get
        } else {
          true
        }

        checkEnd && checkSequence
      }

      override def hasNext: Boolean = {
        while (currKey != null && !inRange(currKey)) {
          next()
        }
        currKey != null
      }

      override def next(): MapKey = {
        val saveKey = currKey
        currKey = impl.higherKey(saveKey)
        saveKey
      }
    }
  }

  // dreadful
  def clear: Unit = {
    impl.clear()
  }
}

object PersistentMap {
  val instance: PersistentMap = new PersistentMap
}
