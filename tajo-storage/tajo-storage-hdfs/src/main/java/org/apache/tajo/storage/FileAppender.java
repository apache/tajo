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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.NotImplementedException;

import java.io.IOException;
import java.util.List;

public abstract class FileAppender implements Appender {
  protected boolean inited = false;

  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final Path workDir;
  protected final TaskAttemptId taskAttemptId;

  protected boolean tableStatsEnabled;
  protected boolean[] columnStatsEnabled;
  protected Path path;

  public FileAppender(Configuration conf, TaskAttemptId taskAttemptId, Schema schema,
                      TableMeta meta, Path workDir) {
    this.conf = conf;
    this.meta = meta;
    this.schema = schema;
    this.workDir = workDir;
    this.taskAttemptId = taskAttemptId;


    if (taskAttemptId != null) {
      if (!(conf instanceof TajoConf)) {
        throw new IllegalArgumentException("Configuration must be an instance of TajoConf");
      }

      FileTablespace space = TablespaceManager.get(workDir.toUri());
      this.path = space.getAppenderFilePath(taskAttemptId, workDir);

    } else {
      this.path = workDir;
    }
  }

  @Override
  public void init() throws IOException {
    if (inited) {
     throw new IllegalStateException("FileAppender is already initialized.");
    }
    inited = true;
  }

  @Override
  public void enableStats() {
    if (inited) {
      throw new IllegalStateException("Should enable this option before init()");
    }

    this.tableStatsEnabled = true;
    this.columnStatsEnabled = new boolean[schema.size()];
  }

  @Override
  public void enableStats(List<Column> columnList) {
    enableStats();
    columnList.forEach(column -> columnStatsEnabled[schema.getIndex(column)] = true);
  }

  @Override
  public long getEstimatedOutputSize() throws IOException {
    return getOffset();
  }

  public long getOffset() throws IOException {
    throw new IOException(new NotImplementedException());
  }
}
