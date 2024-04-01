package org.apache.iceberg;

import java.io.Closeable;
import java.util.List;

public interface ManifestEntryAppender<D> extends Closeable {
  void add(D datum);

  void add(D addFile, long dataSequenceNumber);

  List<ManifestFile> toManifestFiles();

  default void commit(long sequenceNumber) {
  }
}
