package org.apache.iceberg

import java.nio.ByteBuffer
import java.util.Collection

import org.apache.iceberg.CutOpType
import org.apache.iceberg.ManifestReader.FileType
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.CloseableIterator
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.io.QDTreeKvdbInputFile
import org.apache.iceberg.metrics.ScanMetrics
import org.apache.iceberg.types.Type.TypeID._
import org.apache.iceberg.util.KeyType
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.PartitionSet
import org.apache.iceberg.util.PersistentMap

class QDTreeManifestReader[F <: ContentFile[F]](
  val manifestPath: String,
  val inheritableMetadata: InheritableMetadata,
  val content: FileType) extends ManifestFileParser[F] {
  private lazy val metaStore = PersistentMap.instance
  private lazy val fileSchema = new Schema(DataFile.getType(spec.partitionType()).fields())
  private lazy val sequenceNumber = manifestPath.split("-")(1).toLong

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
    new CloseableIterable[ManifestEntry[F]] {
      private val smallKey = {
        val minCut = new QDTreeCut(-1, CutOpType.All, BOOLEAN, null, null)
        val byteBuffer = ByteBuffer.allocateDirect(32)
        QDTreeCut.toByteBuffer(byteBuffer, minCut)
        byteBuffer.flip()
        new MapKey(domain = KeyType.CutSequence, byteBuf = byteBuffer, snapSequence = 0)
      }

      def iterator(): CloseableIterator[ManifestEntry[F]] = {
        new CloseableIterator[ManifestEntry[F]] {
          private val keyIter = metaStore.iterator(Some(smallKey), None, Some(sequenceNumber))
          private var oldReader: ManifestReader[F] = null
          private var entryIter = getNextReaderIter

          private def getNextReaderIter: CloseableIterator[ManifestEntry[F]] = {
            if (oldReader != null) {
              oldReader.close()
            }

            if (keyIter.hasNext) {
              val fileKey = keyIter.next()
              val inputFile = new QDTreeKvdbInputFile(fileKey, metaStore)
              oldReader = new ManifestReader[F](inputFile, 0, null, inheritableMetadata, content)
              oldReader.entries().iterator()
            } else {
              null
            }
          }

          def hasNext(): Boolean = {
            while (entryIter != null && !entryIter.hasNext()) {
              entryIter = getNextReaderIter
            }
            entryIter != null && entryIter.hasNext()
          }

          def next(): ManifestEntry[F] = {
            if (entryIter != null) {
              entryIter.next()
            } else {
              null
            }
          }

          def close(): Unit = {
            if (oldReader != null) {
              oldReader.close()
            }
          }
        }
      }

      def close(): Unit = {
      }
    }
  }

  override def liveEntries: CloseableIterable[ManifestEntry[F]] = {
    throw new UnsupportedOperationException("doesn't support yet")
  }

  override def close(): Unit = {
  }
}
