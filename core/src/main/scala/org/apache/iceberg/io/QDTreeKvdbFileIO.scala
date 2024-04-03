package org.apache.iceberg.io

import org.apache.iceberg.io.QDTreeKvdbInputFile
import org.apache.iceberg.io.QDTreeKvdbOutputFile
import org.apache.iceberg.util.PersistentMap

object QDTreeKvdbFileIO extends FileIO {
  lazy val metaStore = PersistentMap.instance

  override def newInputFile(path: String): InputFile = {
    new QDTreeKvdbInputFile(QDTreeKvdbUtils.getKeyFromPath(path), metaStore)
  }

  override def newOutputFile(path: String): OutputFile = {
    new QDTreeKvdbOutputFile(QDTreeKvdbUtils.getKeyFromPath(path), metaStore)
  }

  override def deleteFile(path: String): Unit = {
    metaStore.delete(QDTreeKvdbUtils.getKeyFromPath(path))
  }
}
