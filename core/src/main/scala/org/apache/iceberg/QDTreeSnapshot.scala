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
import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.util.{List => JList}
import java.util.{Map => JMap}
import org.apache.iceberg.io.ByteBufferInputFile
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.util.KeyType
import org.apache.iceberg.util.MapKey
import org.apache.iceberg.util.PersistentMap

import scala.jdk.CollectionConverters._

class QDTreeSnapshot(
  val sequenceNumber: Long,
  val snapshotId: Long,
  val parentId: JLong,
  val timestampMillis: Long,
  val operation: String,
  val summary: JMap[String, String],
  val manifestListLocation: String,
  override val schemaId: Integer) extends Snapshot {
  private val metaStore: PersistentMap = PersistentMap.instance

  override def allManifests(io: FileIO): JList[ManifestFile] = {
    dataManifests(io)
  }

  override def dataManifests(io: FileIO): JList[ManifestFile] = {
    dataManifestFile
  }

  override def deleteManifests(io: FileIO): JList[ManifestFile] = {
    deleteManifestFile
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

  private lazy val dataManifestFile = {
    val valBytes = metaStore.getVal(QDTreeSnapshot.dataManifestFileKey(sequenceNumber))
    ManifestLists.read(new ByteBufferInputFile(List(valBytes).asJava))
  }

  private lazy val deleteManifestFile = {
    val valBytes = metaStore.getVal(QDTreeSnapshot.deleteManifestFileKey(sequenceNumber))
    ManifestLists.read(new ByteBufferInputFile(List(valBytes).asJava))
  }
}

object QDTreeSnapshot {
  val dataManifestFileKeyTemplate = "manifestfile-%d-data"
  val deleteManifestFileKeyTemplate = "manifestfile-%d-delete"

  def dataManifestFileKey(sequenceNumber: Long): MapKey = {
    new MapKey(domain = KeyType.ByteArray, byteBuf = ByteBuffer.wrap(dataManifestFileKeyTemplate.format(sequenceNumber).getBytes), snapSequence = sequenceNumber)
  }

  def deleteManifestFileKey(sequenceNumber: Long): MapKey = {
    new MapKey(domain = KeyType.ByteArray, byteBuf = ByteBuffer.wrap(deleteManifestFileKeyTemplate.format(sequenceNumber).getBytes), snapSequence = sequenceNumber)
  }
}
