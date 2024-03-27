package org.apache.iceberg

import java.util.{Collection => JCollection}

import org.apache.iceberg.ManifestEntry
import org.apache.iceberg.ManifestEntry.Status

import org.apache.iceberg.types.Types.NestedField.optional
import org.apache.iceberg.types.Types.NestedField.required

import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.StructType

trait QDTreeManifestEntry[F <: ContentFile[F]] {
  def isLive: Boolean = {
    status == Status.ADDED || status == Status.EXISTING
  }

  def status: Status
  def snapshotId: Option[Long]
  def dataSequenceNumber: Option[Long]
  def fileSequenceNumber: Option[Long]
  def dataSet: JCollection[F]

  def copy: QDTreeManifestEntry[F]
  def copyWithoutStats: QDTreeManifestEntry[F]
}

object QDTreeManifestEntry {
  // Reuse ManifestEntry fields
  val DATA_SET_ID: Int = ManifestEntry.DATA_FILE_ID + 1
  val DATA_FILE_ELEMENT_ID: Int = DATA_SET_ID + 1

  def getSchema: Schema = {
    wrapFileSchema(V2Metadata.fileType(PartitionSpec.unpartitioned().partitionType()))
  }

  private def wrapFileSchema(fileType: StructType): Schema = {
    new Schema(
      ManifestEntry.STATUS,
      ManifestEntry.SNAPSHOT_ID,
      ManifestEntry.SEQUENCE_NUMBER,
      ManifestEntry.FILE_SEQUENCE_NUMBER,
      required(
        DATA_SET_ID,
        "data_set",
        Types.ListType.ofRequired(
          DATA_FILE_ELEMENT_ID,
          fileType)))
  }
}
