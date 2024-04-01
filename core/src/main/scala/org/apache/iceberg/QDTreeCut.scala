package org.apache.iceberg

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

import java.lang.{Boolean => JBoolean}
import java.lang.{Double => JDouble}
import java.lang.{Float => JFloat}
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}

import java.math.BigDecimal
import java.math.BigInteger

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Type.TypeID._
import org.apache.iceberg.expressions.Expression.Operation

import org.apache.iceberg.util.KeyType
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.UUIDUtil

object CutOpType extends Enumeration {
  case class OpType(opId: Int) extends super.Val {}
  import scala.language.implicitConversions
  implicit def valueToOpTypeVal(x: CutOpType.Value): CutOpType.OpType = x.asInstanceOf[CutOpType.OpType]

  val All = OpType(0)
  val LessThan = OpType(1)
  val LargerEqualPlusLessThan = OpType(2)
  val LargerEqual = OpType(3)
  val Equal     = OpType(4)
  val NotEqual  = OpType(5)
}

class QDTreeCut(
  val columnId: Int,
  val opType: CutOpType.OpType,
  val argType: TypeID,
  val arg1: AnyRef,
  val arg2: AnyRef) extends Ordered[QDTreeCut] {

  def compare(that: QDTreeCut): Int = {
    if (this eq that) {
      return 0
    }

    if (columnId != that.columnId) {
      columnId - that.columnId
    } else if (opType == that.opType && opType == CutOpType.All) {
      0
    } else {
      throw new UnsupportedOperationException(classOf[QDTreeCut].getName)
    }
  }
}

class QDTreeCutIterator(byteArr: ByteBuffer) extends Iterator[QDTreeCut] {
  override def hasNext: Boolean = {
    byteArr.remaining() > 0
  }

  override def next(): QDTreeCut = {
    QDTreeCut.fromByteBuffer(byteArr)
  }
}

object QDTreeCut {
  private val DefaultDecimalScale = 0

  private def compareCuts(obj1: MapKey, obj2: MapKey, withSequence: Boolean): Int = {
    if (obj1 eq obj2) {
      return 0
    }

    require(obj1.domain == KeyType.CutSequence && obj2.domain == KeyType.CutSequence)
    val min = new QDTreeCut(-1, CutOpType.All, BOOLEAN, null, null)
    val cuts1 = new QDTreeCutIterator(obj1.getBytes)
    val cuts2 = new QDTreeCutIterator(obj2.getBytes)
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
      if (withSequence) {
        Some(obj1.snapSequence.compare(obj2.snapSequence))
      } else {
        Some(0)
      }
    }).get
  }

  def getArgument(byteBuffer: ByteBuffer, argType: TypeID): AnyRef = {
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    argType match {
      case BOOLEAN => {
        val oneByte = Array.ofDim[Byte](1)
        byteBuffer.get(oneByte)
        JBoolean.valueOf(oneByte(0) != 0) 
      }
      case INTEGER | DATE => {
        val intVal = byteBuffer.getInt()
        JInteger.valueOf(intVal)
      }
      case LONG | TIME | TIMESTAMP => {
        val longVal = byteBuffer.getLong()
        JLong.valueOf(longVal)
      }
      case FLOAT => {
        val floatVal = byteBuffer.getFloat()
        JFloat.valueOf(floatVal)
      }
      case DOUBLE => {
        val doubleVal = byteBuffer.getDouble()
        JDouble.valueOf(doubleVal)
      }
      case UUID => {
        byteBuffer.order(ByteOrder.BIG_ENDIAN)
        UUIDUtil.convert(byteBuffer)
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
      }
      case STRING => {
        val len = byteBuffer.getInt()
        val bytes = Array.ofDim[Byte](len)
        byteBuffer.get(bytes)
        bytes.map(_.asInstanceOf[Char]).mkString
      }
      case FIXED | BINARY => {
        val len = byteBuffer.getInt()
        val bytes = Array.ofDim[Byte](len)
        byteBuffer.get(bytes)
        ByteBuffer.wrap(bytes)
      }
      case DECIMAL => {
        val len = byteBuffer.getInt()
        val bytes = Array.ofDim[Byte](len)
        byteBuffer.get(bytes)
        // Calibrate scale before use
        new BigDecimal(new BigInteger(bytes), DefaultDecimalScale)
      }
      case _ => throw new UnsupportedOperationException(s"not supported type ${argType}")
    }
  }

  def putArgument(byteBuffer: ByteBuffer, typeId: TypeID, arg: AnyRef): Unit = {
    throw new UnsupportedOperationException("please implement this function")
  }

  def fromByteBuffer(byteBuffer: ByteBuffer): QDTreeCut = {
    val magic = byteBuffer.getInt()
    assert(magic == 0xface)
    val columnId = byteBuffer.getInt()
    val opTypeId = byteBuffer.getInt()
    val opType = CutOpType.values.find((enumType: CutOpType.Value) => enumType.opId == opTypeId).get
    var arg1: AnyRef = null
    var arg2: AnyRef = null

    if (opType == CutOpType.All) {
      return new QDTreeCut(columnId, opType, TypeID.BOOLEAN, null, null)
    }

    // Must have at least one argument
    val typeStrLen = byteBuffer.getInt()
    val enumName = Array.ofDim[Byte](typeStrLen)
    byteBuffer.get(enumName)
    val argTypeId = TypeID.valueOf(enumName.mkString)

    opType match {
      case CutOpType.LessThan | CutOpType.LargerEqual | CutOpType.Equal | CutOpType.NotEqual => {
        arg1 = getArgument(byteBuffer, argTypeId)
      }
      case CutOpType.LargerEqualPlusLessThan => {
        arg1 = getArgument(byteBuffer, argTypeId)
        arg2 = getArgument(byteBuffer, argTypeId)
      }
      case _ => throw new UnsupportedOperationException(s"not supported $opType")
    }

    new QDTreeCut(columnId, opType, argTypeId, arg1, arg2)
  }

  def toByteBuffer(byteBuffer: ByteBuffer, cut: QDTreeCut): Unit = {
    byteBuffer.putInt(0xface).putInt(cut.columnId).putInt(cut.opType.opId)
    if (cut.opType == CutOpType.All) {
      return
    }

    val bytes = cut.argType.name().getBytes("ISO-8859-1")
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)

    cut.opType match {
      case CutOpType.LessThan | CutOpType.LargerEqual | CutOpType.Equal | CutOpType.NotEqual => {
        putArgument(byteBuffer, cut.argType, cut.arg1)
      }
      case CutOpType.LargerEqualPlusLessThan => {
        putArgument(byteBuffer, cut.argType, cut.arg1)
        putArgument(byteBuffer, cut.argType, cut.arg2)
      }
      case _ => throw new UnsupportedOperationException("Unkown QDTreeCut operation type")
    }
  }

  MapKey.registerComparator(KeyType.CutSequence, compareCuts)
}
