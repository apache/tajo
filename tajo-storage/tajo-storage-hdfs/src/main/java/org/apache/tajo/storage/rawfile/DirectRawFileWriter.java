/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.rawfile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.unit.StorageUnit;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class DirectRawFileWriter extends FileAppender {
  public static final String FILE_EXTENSION = "draw";
  private static final int BUFFER_SIZE = 64 * StorageUnit.KB;
  private static final Log LOG = LogFactory.getLog(DirectRawFileWriter.class);

  private FileChannel channel;
  private RandomAccessFile randomAccessFile;
  private FSDataOutputStream fos;
  private boolean isLocal;
  private long pos;

  private TableStatistics stats;
  private ShuffleType shuffleType;
  private MemoryRowBlock memoryRowBlock;

  public DirectRawFileWriter(Configuration conf, TaskAttemptId taskAttemptId,
                             final Schema schema, final TableMeta meta, final Path path) throws IOException {
    super(conf, taskAttemptId, schema, meta, path);
  }

  @Override
  public void init() throws IOException {
    File file;
    FileSystem fs = path.getFileSystem(conf);

    if (fs instanceof LocalFileSystem) {
      try {
        if (path.toUri().getScheme() != null) {
          file = new File(path.toUri());
        } else {
          file = new File(path.toString());
        }
      } catch (IllegalArgumentException iae) {
        throw new IOException(iae);
      }

      randomAccessFile = new RandomAccessFile(file, "rw");
      channel = randomAccessFile.getChannel();
      isLocal = true;
    } else {
      fos = fs.create(path, true);
      isLocal = false;
    }

    if (enabledStats) {
      this.stats = new TableStatistics(this.schema);
      this.shuffleType = PlannerUtil.getShuffleType(
          meta.getOption(StorageConstants.SHUFFLE_TYPE,
              PlannerUtil.getShuffleType(ShuffleType.NONE_SHUFFLE)));
    }

    memoryRowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(schema), BUFFER_SIZE, true);
    pos = 0;
    super.init();
  }

  @Override
  public long getOffset() throws IOException {
    return pos + memoryRowBlock.getMemory().writerPosition();
  }

  public void writeRowBlock(MemoryRowBlock rowBlock) throws IOException {
    if(isLocal) {
      pos += rowBlock.getMemory().writeTo(channel);
    } else {
      pos += rowBlock.getMemory().writeTo(fos);
    }

    rowBlock.getMemory().clear();

    if (enabledStats) {
      stats.incrementRows(rowBlock.rows() - stats.getNumRows());
    }
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    if (shuffleType == ShuffleType.RANGE_SHUFFLE) {
      // it is to calculate min/max values, and it is only used for the intermediate file.
      for (int i = 0; i < schema.size(); i++) {
        stats.analyzeField(i, t);
      }
    }

    memoryRowBlock.getWriter().addTuple(t);

    if(memoryRowBlock.getMemory().readableBytes() >= BUFFER_SIZE) {
      writeRowBlock(memoryRowBlock);
    }
  }

  @Override
  public void flush() throws IOException {
    if(memoryRowBlock.getMemory().isReadable()) {
      writeRowBlock(memoryRowBlock);
    }
  }

  @Override
  public void close() throws IOException {
    flush();

    if (enabledStats) {
      stats.setNumBytes(getOffset());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("RawFileAppender written: " + getOffset() + " bytes, path: " + path);
    }

    IOUtils.cleanup(LOG, channel, randomAccessFile, fos);
    memoryRowBlock.release();
  }

  @Override
  public TableStats getStats() {
    if (enabledStats) {
      stats.setNumBytes(pos);
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
