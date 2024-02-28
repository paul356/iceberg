package org.apache.iceberg

import com.fasterxml.jackson.databind.JsonNode
import java.lang.{Long => JLong}
import java.lang.{Integer => JInteger}
import java.util.{List => JList}
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.JsonUtil
import org.apache.iceberg.util.PersistentMap

class QDTreeManifestFile(
  val sequenceNumber: Long,
  val snapshotId: JLong,
  val kvStore: PersistentMap) extends ManifestFile {

  private class ManifestFileInfo(
    val addedFilesCount: Option[Int],
    val existingFilesCount: Option[Int],
    val deletedFilesCount: Option[Int],
    val addedRowsCount: Option[Long],
    val existingRowsCount: Option[Long],
    val deletedRowsCount: Option[Long]) {
  }

  import QDTreeManifestFile.{getInt, getLong}

  private lazy val summaries = {
    val fileKey = QDTreeManifestFile.manifestFileKey(sequenceNumber)
    val valStr = kvStore.getVal(fileKey).mkString
    JsonUtil.parse(valStr, (jsonNode) => {
      val addedFiles = getInt(jsonNode, QDTreeManifestFile.AddedFilesCount)
      val existingFiles = getInt(jsonNode, QDTreeManifestFile.ExistingFilesCount)
      val deletedFiles = getInt(jsonNode, QDTreeManifestFile.DeletedFilesCount)
      val addedRows = getLong(jsonNode, QDTreeManifestFile.AddedRowsCount)
      val existingRows = getLong(jsonNode, QDTreeManifestFile.ExistingRowsCount)
      val deletedRows = getLong(jsonNode, QDTreeManifestFile.DeletedRowsCount)
      new ManifestFileInfo(
        addedFiles,
        existingFiles,
        deletedFiles,
        addedRows,
        existingRows,
        deletedRows)
    })
  }

  override def path: String = {
    "file:///nonexist"
  }

  override def length: Long = {
    0
  }

  override def partitionSpecId: Int = {
    0
  }

  override def content: ManifestContent = {
    ManifestContent.DATA
  }

  override def minSequenceNumber: Long = {
    sequenceNumber
  }

  override def addedFilesCount: JInteger = {
    if (summaries.addedFilesCount.isDefined) summaries.addedFilesCount.get else null
  }

  override def addedRowsCount: JLong = {
    if (summaries.addedRowsCount.isDefined) summaries.addedRowsCount.get else null
  }

  override def existingFilesCount: JInteger = {
    if (summaries.existingFilesCount.isDefined) summaries.existingFilesCount.get else null
  }

  override def existingRowsCount: JLong = {
    if (summaries.existingRowsCount.isDefined) summaries.existingRowsCount.get else null
  }

  override def deletedFilesCount: JInteger = {
    if (summaries.deletedFilesCount.isDefined) summaries.deletedFilesCount.get else null
  }

  override def deletedRowsCount: JLong = {
    if (summaries.deletedRowsCount.isDefined) summaries.deletedRowsCount.get else null
  }

  override def partitions: JList[ManifestFile.PartitionFieldSummary] = {
    null
  }

  override def copy: ManifestFile = {
    return new QDTreeManifestFile(sequenceNumber, snapshotId, kvStore);
  }
}

object QDTreeManifestFile {
  private val ManifestFileKeyTemplate = "manifestfile-%d"
  // json fields
  private val AddedFilesCount = "added-files-count"
  private val ExistingFilesCount = "existing-files-count"
  private val DeletedFilesCount = "deleted-files-count"

  private val AddedRowsCount = "added-rows-count"
  private val ExistingRowsCount = "existing-rows-count"
  private val DeletedRowsCount = "deleted-rows-count"

  private def getInt(node: JsonNode, property: String): Option[Int] = {
    if (node.hasNonNull(property)) Some(JsonUtil.getInt(property, node)) else None
  }

  private def getLong(node: JsonNode, property: String): Option[Long] = {
    if (node.hasNonNull(property)) Some(JsonUtil.getLong(property, node)) else None
  }

  def manifestFileKey(sequenceNumber: Long) = {
    new MapKey(ManifestFileKeyTemplate.format(sequenceNumber).getBytes, sequenceNumber)
  }
}
