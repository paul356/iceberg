package org.apache.iceberg

import org.apache.iceberg.ManifestEntry.Status

import org.apache.iceberg.types.Types.NestedField.optional
import org.apache.iceberg.types.Types.NestedField.required

import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.StructType

abstract class QDTreeManifestEntry[F <: ContentFile[F]](
  val status: Status,
  var snapshotId: Option[Long],
  var dataSequenceNumber: Option[Long],
  var fileSequenceNumber: Option[Long],
  val dataSet: List[F]
){
  def isLive: Boolean = {
    status == Status.ADDED || status == Status.EXISTING
  }

  def copy: QDTreeManifestEntry[F]

  def copyWithoutStats: QDTreeManifestEntry[F]
}

object QDTreeManifestEntry {
    // ids for data-file columns are assigned from 1000
  val STATUS: Types.NestedField = required(0, "status", Types.IntegerType.get())
  val SNAPSHOT_ID: Types.NestedField = optional(1, "snapshot_id", Types.LongType.get())
  val SEQUENCE_NUMBER: Types.NestedField = optional(3, "sequence_number", Types.LongType.get())
  val FILE_SEQUENCE_NUMBER: Types.NestedField = optional(4, "file_sequence_number", Types.LongType.get());
  val DATA_SET_ID: Int = 5;
  val DATA_FILE_ELEMENT_ID: Int = 6;
  // next ID to assign: 7

  def getSchema(partitionType: StructType): Schema = {
    wrapFileSchema(DataFile.getType(partitionType))
  }

  def wrapFileSchema(fileType: StructType): Schema = {
    new Schema(
      STATUS,
      SNAPSHOT_ID,
      SEQUENCE_NUMBER,
      FILE_SEQUENCE_NUMBER,
      required(
        DATA_SET_ID,
        "data_set",
        Types.ListType.ofRequired(
          DATA_FILE_ELEMENT_ID,
          fileType)))
  }
}
