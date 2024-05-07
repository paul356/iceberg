package org.apache.iceberg.io

import org.apache.iceberg.io.QDTreeKvdbInputFile
import org.apache.iceberg.io.QDTreeKvdbOutputFile
import org.apache.iceberg.util.PersistentMap

class QDTreeKvdbFileIO(delegate: FileIO) extends FileIO {
  @transient lazy val metaStore = PersistentMap.instance

  private def nonDelegateFiles(path: String): Boolean = {
    path.endsWith(".avro")
  }

  override def newInputFile(path: String): InputFile = {
    if (nonDelegateFiles(path)) {
      new QDTreeKvdbInputFile(QDTreeKvdbUtils.getKeyFromPath(path), metaStore)
    } else {
      delegate.newInputFile(path)
    }
  }

  override def newOutputFile(path: String): OutputFile = {
    if (nonDelegateFiles(path)) {
      new QDTreeKvdbOutputFile(QDTreeKvdbUtils.getKeyFromPath(path), metaStore)
    } else {
      delegate.newOutputFile(path)
    }
  }

  override def deleteFile(path: String): Unit = {
    if (nonDelegateFiles(path)) {
      metaStore.delete(QDTreeKvdbUtils.getKeyFromPath(path))
    } else {
      delegate.deleteFile(path)
    }
  }
}
