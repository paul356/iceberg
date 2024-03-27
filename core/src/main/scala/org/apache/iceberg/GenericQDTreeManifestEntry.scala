package org.apache.iceberg

import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}

import java.util.{Collection => JCollection}

import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.specific.SpecificData.SchemaConstructable

import org.apache.iceberg.ManifestEntry.Status

class GenericQDTreeManifestEntry[F <: ContentFile[F]](
  schema: AvroSchema) extends QDTreeManifestEntry[F] with IndexedRecord with SchemaConstructable {
  private var innerStatus: Status = null
  private var innerSnapshotId: Option[Long] = Option.empty[Long]
  private var innerDataSequenceNumber: Option[Long] = Option.empty[Long]
  private var innerFileSequenceNumber: Option[Long] = Option.empty[Long]
  private var innerDataSet: JCollection[F] = null

  def this() = {
    this(null)
  }

  override def status: Status = {
    innerStatus
  }

  override def snapshotId: Option[Long] = {
    innerSnapshotId
  }

  override def dataSequenceNumber: Option[Long] = {
    innerDataSequenceNumber
  }

  override def fileSequenceNumber: Option[Long] = {
    innerFileSequenceNumber
  }

  override def dataSet: JCollection[F] = {
    innerDataSet
  }

  override def getSchema(): AvroSchema = {
    schema
  }

  override def get(i: Int): AnyRef = {
    i match {
      case 0 => JInteger.valueOf(innerStatus.id())
      case 1 => JLong.valueOf(innerSnapshotId.get)
      case 2 => JLong.valueOf(innerDataSequenceNumber.get)
      case 3 => JLong.valueOf(innerFileSequenceNumber.get)
      case 4 => innerDataSet
    }
  }

  override def put(i: Int, v: AnyRef): Unit = {
    i match {
      case 0 => innerStatus = Status.values()(v.asInstanceOf[JInteger].intValue())
      case 1 => if (v != null) {
        innerSnapshotId = Some(v.asInstanceOf[JLong].longValue())
      }
      case 2 => if (v != null) {
        innerDataSequenceNumber = Some(v.asInstanceOf[JLong].longValue())
      }
      case 3 => if (v != null) {
        innerFileSequenceNumber = Some(v.asInstanceOf[JLong].longValue())
      }
      case 4 => innerDataSet = v.asInstanceOf[JCollection[F]]
    }
  }

  override def copy: QDTreeManifestEntry[F] = {
    null
  }

  override def copyWithoutStats: QDTreeManifestEntry[F] = {
    null
  }
}
