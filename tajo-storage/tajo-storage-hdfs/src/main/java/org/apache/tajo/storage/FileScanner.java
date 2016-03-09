/**
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

package org.apache.tajo.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;

public abstract class FileScanner implements Scanner {
  private static final Log LOG = LogFactory.getLog(FileScanner.class);

  protected boolean inited = false;
  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final FileFragment fragment;
  protected final int columnNum;

  protected Column [] targets;

  protected float progress;

  protected TableStats tableStats;

  public FileScanner(Configuration conf, final Schema schema, final TableMeta meta, final Fragment fragment) {
    this.conf = conf;
    this.meta = meta;
    this.schema = schema;
    this.fragment = (FileFragment)fragment;
    this.tableStats = new TableStats();
    this.columnNum = this.schema.size();
  }

  public void init() throws IOException {
    inited = true;
    progress = 0.0f;

    if (fragment != null) {
      tableStats.setNumBytes(fragment.getLength());
      tableStats.setNumBlocks(1);
    }

    if (schema != null) {
      for(Column eachColumn: schema.getRootColumns()) {
        ColumnStats columnStats = new ColumnStats(eachColumn);
        tableStats.addColumnStat(columnStats);
      }
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void pushOperators(LogicalNode planPart) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void setTarget(Column[] targets) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }
    this.targets = targets;
  }

  @Override
  public void setLimit(long num) {
  }

  public static FileSystem getFileSystem(TajoConf tajoConf, Path path) throws IOException {
    return FileSystem.get(path.toUri(), tajoConf);
  }

  @Override
  public float getProgress() {
    return progress;
  }

  @Override
  public TableStats getInputStats() {
    return tableStats;
  }
}
