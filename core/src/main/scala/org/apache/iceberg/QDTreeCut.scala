package org.apache.iceberg

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.expressions.Expression.Operation

import org.apache.iceberg.util.KeyType
import org.apache.iceberg.util.MapKey

object CutOpType extends Enumeration {
  case class OpType(opId: Int) extends super.Val {}
  import scala.language.implicitConversions
  implicit def valueToOpTypeVal(x: Value): OpType = x.asInstanceOf[OpType]

  val True = OpType(0)
  val LessThan = OpType(1)
  val LargerEqualPlusLessThan = OpType(2)
  val LargerEqual = OpType(3)
  val Equal     = OpType(4)
  val NotEqual  = OpType(5)
}

class QDTreeCut(
  val columnId: Int,
  val op: CutOpType.OpType,
  val arg1: Serializable,
  val arg2: Serializable) extends Serializable with Ordered[QDTreeCut] {

  def compare(that: QDTreeCut): Int = {
    if (columnId != that.columnId) {
      columnId - that.columnId
    } else if (op.opId != that.op.opId) {
      op.opId - that.op.opId
    } else {
      throw new UnsupportedOperationException(classOf[QDTreeCut].getName)
    }
  }
}

class QDTreeCutIterator(byteArr: Array[Byte]) extends Iterator[QDTreeCut] {
  lazy val stream = new ObjectInputStream(new ByteArrayInputStream(byteArr))

  override def hasNext: Boolean = {
    stream.available() > 0
  }

  override def next(): QDTreeCut = {
    val obj = stream.readObject
    obj.asInstanceOf[QDTreeCut]
  }
}

object QDTreeCut {
  private def compareCuts(obj1: MapKey, obj2: MapKey): Int = {
    require(obj1.domain == KeyType.CutSequence && obj2.domain == KeyType.CutSequence)
    val min = new QDTreeCut(-1, CutOpType.LessThan, null, null)
    val cuts1 = new QDTreeCutIterator(obj1.bytes)
    val cuts2 = new QDTreeCutIterator(obj2.bytes)
    val zip = cuts1.zipAll(cuts2, min, min)
    val inequalPair = zip.find({case (cut1, cut2) => cut1 != cut2})
    val cmpReslt = inequalPair.map({
      case (cut1, cut2) =>
        if (cut1 < cut2) {
          -1
        } else {
          1
        }
    })
    cmpReslt.orElse({
      Some(obj1.snapSequence.compare(obj2.snapSequence))
    }).get
  }

  MapKey.registerComparator(KeyType.CutSequence, compareCuts)
}
