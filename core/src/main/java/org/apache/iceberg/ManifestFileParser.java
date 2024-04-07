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
