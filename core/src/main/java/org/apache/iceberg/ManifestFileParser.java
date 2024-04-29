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
package org.apache.iceberg;

import java.util.Collection;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.util.PartitionSet;

public interface ManifestFileParser<F extends ContentFile<F>> extends CloseableIterable<F> {
  boolean isDeleteManifestReader();

  InputFile file();

  Schema schema();

  PartitionSpec spec();

  ManifestFileParser<F> select(Collection<String> expr);

  ManifestFileParser<F> project(Schema newFileProjection);

  ManifestFileParser<F> filterPartitions(Expression expr);

  ManifestFileParser<F> filterPartitions(PartitionSet partitions);

  ManifestFileParser<F> filterRows(Expression expr);

  ManifestFileParser<F> caseSensitive(boolean isCseSensitive);

  ManifestFileParser<F> scanMetrics(ScanMetrics newScanMetrics);

  CloseableIterator<F> iterator();

  CloseableIterable<ManifestEntry<F>> entries();

  CloseableIterable<ManifestEntry<F>> liveEntries();
}
