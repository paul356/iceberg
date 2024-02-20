/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg

import scala.jdk.CollectionConverters._
import java.lang.Integer
import java.lang.{Iterable => JIterable}
import java.util.{List => JList}
import java.lang.{Long => JLong}
import java.util.{Map => JMap}
import org.apache.iceberg.io.FileIO

class QDTreeSnapshot(
  val sequenceNumber: Long,
  val snapshotId: Long,
  val parentId: JLong,
  val timestampMillis: Long,
  val operation: String,
  val summary: JMap[String, String],
  val manifestListLocation: String,
  override val schemaId: Integer) extends Snapshot {
  override def allManifests(io: FileIO): JList[ManifestFile] = {
    throw new UnsupportedOperationException(classOf[QDTreeSnapshot].getName)
  }

  override def dataManifests(io: FileIO): JList[ManifestFile] = {
    throw new UnsupportedOperationException(classOf[QDTreeSnapshot].getName)
  }

  override def deleteManifests(io: FileIO): JList[ManifestFile] = {
    throw new UnsupportedOperationException(classOf[QDTreeSnapshot].getName)
  }

  override def addedDataFiles(io: FileIO): JIterable[DataFile] = {
    List.empty[DataFile].asJava
  }

  override def removedDataFiles(io: FileIO): JIterable[DataFile] = {
    List.empty[DataFile].asJava
  }

  override def addedDeleteFiles(io: FileIO): JIterable[DeleteFile] = {
    List.empty[DeleteFile].asJava
  }

  override def removedDeleteFiles(io: FileIO): JIterable[DeleteFile] = {
    List.empty[DeleteFile].asJava
  }
}
