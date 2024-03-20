package org.apache.iceberg

import java.nio.ByteBuffer
import java.util.{Map => JMap}

class QDTreeCutInclusiveEvaluator(
  val schema: Schema,
  val lowerBound: JMap[Integer, ByteBuffer],
  val upperBound: JMap[Integer, ByteBuffer]){
  def checkAgainstCut(cut: QDTreeCut): InclusiveType.Value = {
    cut.op match {
      case CutOpType.True => InclusiveType.InCut
      case CutOpType.LessThan => {
        InclusiveType.PartInCut
      }
    }
  }
}

object InclusiveType extends Enumeration {
  val InCut, PartInCut, NotInCut = Value
}

object QDTreeCutInclusiveEvaluator {
  def apply(
    schema: Schema,
    lowerBound: JMap[Integer, ByteBuffer],
    upperBound: JMap[Integer, ByteBuffer]): QDTreeCutInclusiveEvaluator = {
    new QDTreeCutInclusiveEvaluator(schema, lowerBound, upperBound)
  }
}
