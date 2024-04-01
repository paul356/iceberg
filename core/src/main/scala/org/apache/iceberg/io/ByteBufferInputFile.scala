package org.apache.iceberg.io

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{List => JList}

import org.apache.iceberg.io.ByteBufferInputStream
import scala.jdk.CollectionConverters._

class ByteBufferInputFile(byteBufs: JList[ByteBuffer]) extends InputFile {
  override val getLength: Long = byteBufs.asScala.foldLeft(0)((a, b) => a + b.remaining())

  override def newStream: SeekableInputStream = {
    ByteBufferInputStream.wrap(byteBufs)
  }

  override def location: String = {
    "@ByteAraryInputStream"
  }

  override def exists: Boolean = {
    true
  }
}
