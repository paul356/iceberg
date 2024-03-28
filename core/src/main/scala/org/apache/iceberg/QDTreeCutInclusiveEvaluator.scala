package org.apache.iceberg

import java.lang.{Boolean => JBoolean}
import java.lang.{Double => JDouble}
import java.lang.{Float => JFloat}
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.util.Comparator
import java.util.{Map => JMap}

import org.apache.iceberg.types.Comparators
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID._

class QDTreeCutInclusiveEvaluator(
  val schema: Schema,
  val lowerBounds: JMap[Integer, Serializable],
  val upperBounds: JMap[Integer, Serializable]){
  private def checkAgainstCutInner[T](columnId: Int, op: CutOpType.OpType, cmp: Comparator[T], arg1: T, arg2: T): InclusiveType.Value = {
    op match {
      case CutOpType.All => InclusiveType.InCut
      case CutOpType.LessThan => {
        if (!lowerBounds.containsKey(columnId) || !upperBounds.containsKey(columnId)) {
          InclusiveType.NA
        } else {
          val lowerBound = lowerBounds.get(columnId).asInstanceOf[T]
          val upperBound = upperBounds.get(columnId).asInstanceOf[T]

          val cutArg1 = arg1.asInstanceOf[T]
          val res1 = cmp.compare(cutArg1, lowerBound)
          if (res1 < 0 || res1 == 0) {
            InclusiveType.NotInCut
          } else {
            val res2 = cmp.compare(cutArg1, upperBound)
            if (res2 > 0) {
              InclusiveType.InCut
            } else {
              InclusiveType.PartInCut
            }
          }
        }
      }
      case CutOpType.LargerEqual => {
        if (!lowerBounds.containsKey(columnId) || !upperBounds.containsKey(columnId)) {
          InclusiveType.NA
        } else {
          val lowerBound = lowerBounds.get(columnId).asInstanceOf[T]
          val upperBound = upperBounds.get(columnId).asInstanceOf[T]

          val cutArg1 = arg1.asInstanceOf[T]
          val res1 = cmp.compare(cutArg1, lowerBound)
          if (res1 < 0 || res1 == 0) {
            InclusiveType.InCut
          } else {
            val res2 = cmp.compare(cutArg1, upperBound)
            if (res2 > 0) {
              InclusiveType.NotInCut
            } else {
              InclusiveType.PartInCut
            }
          }
        }
      }
      case CutOpType.LargerEqualPlusLessThan => {
        if (!lowerBounds.containsKey(columnId) || !upperBounds.containsKey(columnId)) {
          InclusiveType.NA
        } else {
          val lowerBound = lowerBounds.get(columnId).asInstanceOf[T]
          val upperBound = upperBounds.get(columnId).asInstanceOf[T]
          val cutArg1 = arg1.asInstanceOf[T]
          val cutArg2 = arg2.asInstanceOf[T]

          val res21 = cmp.compare(cutArg2, lowerBound)
          if (res21 < 0 || res21 == 0) {
            InclusiveType.NotInCut
          } else {
            val res12 = cmp.compare(cutArg1, upperBound)
            if (res12 > 0) {
              InclusiveType.NotInCut
            } else {
              val res11 = cmp.compare(cutArg1, lowerBound)
              val res22 = cmp.compare(cutArg2, upperBound)
              if ((res11 < 0 || res11 == 0) && (res22 > 0)) {
                InclusiveType.InCut
              } else {
                InclusiveType.PartInCut
              }
            }
          }
        }
      }
      case _ => throw new UnsupportedOperationException(this.getClass().getName)
    }
  }

  def checkAgainstCut(cut: QDTreeCut): InclusiveType.Value = {
    val fieldType = schema.findField(cut.columnId).`type`()
    val typeId = fieldType.typeId()
    if (fieldType.isInstanceOf[Type.PrimitiveType]) {
      val primitiveType = fieldType.asInstanceOf[Type.PrimitiveType]
      typeId match {
        case BOOLEAN => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[JBoolean], cut.arg2.asInstanceOf[JBoolean])
        case INTEGER => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[JInteger], cut.arg2.asInstanceOf[JInteger])
        case LONG => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[JLong], cut.arg2.asInstanceOf[JLong])
        case FLOAT => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[JFloat], cut.arg2.asInstanceOf[JFloat])
        case DOUBLE => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[JDouble], cut.arg2.asInstanceOf[JDouble])
        case STRING => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[CharSequence], cut.arg2.asInstanceOf[CharSequence])
        case UUID => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[java.util.UUID], cut.arg2.asInstanceOf[java.util.UUID])
        case FIXED => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[ByteBuffer], cut.arg2.asInstanceOf[ByteBuffer])
        case BINARY => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[ByteBuffer], cut.arg2.asInstanceOf[ByteBuffer])
        case DECIMAL => {
          val lowerBound = lowerBounds.get(cut.columnId).asInstanceOf[BigDecimal]
          if (cut.arg1 != null && cut.arg2 != null) {
            checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[BigDecimal].setScale(lowerBound.scale()), cut.arg2.asInstanceOf[BigDecimal].setScale(lowerBound.scale))
          } else if (cut.arg1 != null) {
            checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[BigDecimal].setScale(lowerBound.scale()), cut.arg2.asInstanceOf[BigDecimal])
          } else {
            checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[BigDecimal], cut.arg2.asInstanceOf[BigDecimal])
          }
        }
        case DATE => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[JInteger], cut.arg2.asInstanceOf[JInteger])
        case TIME => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[JLong], cut.arg2.asInstanceOf[JLong])
        case TIMESTAMP => checkAgainstCutInner(cut.columnId, cut.opType, Comparators.forType(primitiveType), cut.arg1.asInstanceOf[JLong], cut.arg2.asInstanceOf[JLong])
        case _ => throw new UnsupportedOperationException(this.getClass().getName())
      }
    } else {
      throw new UnsupportedOperationException("Complex fields sare not supported")
    }
  }
}

object InclusiveType extends Enumeration {
  val InCut, PartInCut, NotInCut, NA = Value
}

object QDTreeCutInclusiveEvaluator {
  def apply(
    schema: Schema,
    lowerBound: JMap[Integer, Serializable],
    upperBound: JMap[Integer, Serializable]): QDTreeCutInclusiveEvaluator = {
    new QDTreeCutInclusiveEvaluator(schema, lowerBound, upperBound)
  }
}
