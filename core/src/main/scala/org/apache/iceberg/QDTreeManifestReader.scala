package org.apache.iceberg

import java.util.Collection

import org.apache.iceberg.ManifestReader.FileType
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.CloseableIterator
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.metrics.ScanMetrics
import org.apache.iceberg.util.PartitionSet

class QDTreeManifestReader[F <: ContentFile[F]](
  val manifestPath: String,
  val content: FileType) extends ManifestFileParser[F] {
  private lazy val fileSchema = new Schema(DataFile.getType(spec.partitionType()).fields())

  override def isDeleteManifestReader = {
    content == FileType.DELETE_FILES
  }

  override def file: InputFile = {
    null
  }

  override def schema: Schema = {
    fileSchema
  }

  override def spec: PartitionSpec = {
    PartitionSpec.unpartitioned()
  }

  override def select(expr: Collection[String]): ManifestFileParser[F] = {
    throw new UnsupportedOperationException("doesn't support select yet")
  }

  override def project(newFileProjection: Schema): ManifestFileParser[F] = {
    throw new UnsupportedOperationException("doesn't support project yet")
  }

  override def filterPartitions(expr: Expression): ManifestFileParser[F] = {
    throw new UnsupportedOperationException("doesn't support filter partitions")
  }

  override def filterPartitions(partitons: PartitionSet): ManifestFileParser[F] = {
    throw new UnsupportedOperationException("doesn't support filter partitions")
  }

  override def filterRows(expr: Expression): ManifestFileParser[F] = {
    throw new UnsupportedOperationException("doesn't support filter rows")
  }

  override def caseSensitive(isCaseSensitive: Boolean): ManifestFileParser[F] = {
    this
  }

  override def scanMetrics(newScanMetrics: ScanMetrics): ManifestFileParser[F] = {
    this
  }

  override def iterator: CloseableIterator[F] = {
    throw new UnsupportedOperationException("doesn't support yet")
  }

  override def entries: CloseableIterable[ManifestEntry[F]] = {
    throw new UnsupportedOperationException("doesn't support yet")
  }

  override def liveEntries: CloseableIterable[ManifestEntry[F]] = {
    throw new UnsupportedOperationException("doesn't support yet")
  }

  override def close(): Unit = {
  }
}
