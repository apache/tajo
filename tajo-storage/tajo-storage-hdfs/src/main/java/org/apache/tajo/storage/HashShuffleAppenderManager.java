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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashShuffleAppenderManager {
  private static final Log LOG = LogFactory.getLog(HashShuffleAppenderManager.class);

  private Map<ExecutionBlockId, Map<Integer, PartitionAppenderMeta>> appenderMap =
      new ConcurrentHashMap<ExecutionBlockId, Map<Integer, PartitionAppenderMeta>>();
  private TajoConf systemConf;
  private FileSystem defaultFS;
  private FileSystem localFS;
  private LocalDirAllocator lDirAllocator;
  private int pageSize;

  public HashShuffleAppenderManager(TajoConf systemConf) throws IOException {
    this.systemConf = systemConf;

    // initialize LocalDirAllocator
    lDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);

    // initialize DFS and LocalFileSystems
    defaultFS = TajoConf.getTajoRootDir(systemConf).getFileSystem(systemConf);
    localFS = FileSystem.getLocal(systemConf);
    pageSize = systemConf.getIntVar(ConfVars.SHUFFLE_HASH_APPENDER_PAGE_VOLUME) * 1024 * 1024;
  }

  public HashShuffleAppender getAppender(TajoConf tajoConf, ExecutionBlockId ebId, int partId,
                              TableMeta meta, Schema outSchema) throws IOException {
    synchronized (appenderMap) {
      Map<Integer, PartitionAppenderMeta> partitionAppenderMap = appenderMap.get(ebId);

      if (partitionAppenderMap == null) {
        partitionAppenderMap = new ConcurrentHashMap<Integer, PartitionAppenderMeta>();
        appenderMap.put(ebId, partitionAppenderMap);
      }

      PartitionAppenderMeta partitionAppenderMeta = partitionAppenderMap.get(partId);
      if (partitionAppenderMeta == null) {
        Path dataFile = getDataFile(ebId, partId);
        FileSystem fs = dataFile.getFileSystem(systemConf);
        if (fs.exists(dataFile)) {
          FileStatus status = fs.getFileStatus(dataFile);
          LOG.info("File " + dataFile + " already exists, size=" + status.getLen());
        }

        if (!fs.exists(dataFile.getParent())) {
          fs.mkdirs(dataFile.getParent());
        }
        FileAppender appender = (FileAppender)((FileStorageManager)StorageManager.getFileStorageManager(tajoConf))
            .getAppender(meta, outSchema, dataFile);
        appender.enableStats();
        appender.init();

        partitionAppenderMeta = new PartitionAppenderMeta();
        partitionAppenderMeta.partId = partId;
        partitionAppenderMeta.dataFile = dataFile;
        partitionAppenderMeta.appender = new HashShuffleAppender(ebId, partId, pageSize, appender);
        partitionAppenderMeta.appender.init();
        partitionAppenderMap.put(partId, partitionAppenderMeta);

        LOG.info("Create Hash shuffle file(partId=" + partId + "): " + dataFile);
      }

      return partitionAppenderMeta.appender;
    }
  }

  public static int getPartParentId(int partId, TajoConf tajoConf) {
    return partId % tajoConf.getIntVar(TajoConf.ConfVars.HASH_SHUFFLE_PARENT_DIRS);
  }

  private Path getDataFile(ExecutionBlockId ebId, int partId) throws IOException {
    try {
      // the base dir for an output dir
      String executionBlockBaseDir = ebId.getQueryId().toString() + "/output" + "/" + ebId.getId() + "/hash-shuffle";
      Path baseDirPath = localFS.makeQualified(lDirAllocator.getLocalPathForWrite(executionBlockBaseDir, systemConf));
      //LOG.info(ebId + "'s basedir is created (" + baseDirPath + ")");

      // If EB has many partition, too many shuffle file are in single directory.
      return StorageUtil.concatPath(baseDirPath, "" + getPartParentId(partId, systemConf), "" + partId);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public List<HashShuffleIntermediate> close(ExecutionBlockId ebId) throws IOException {
    Map<Integer, PartitionAppenderMeta> partitionAppenderMap = null;
    synchronized (appenderMap) {
      partitionAppenderMap = appenderMap.remove(ebId);
    }

    if (partitionAppenderMap == null) {
      LOG.info("Close HashShuffleAppender:" + ebId + ", not a hash shuffle");
      return null;
    }

    // Send Intermediate data to QueryMaster.
    List<HashShuffleIntermediate> intermediateEntries = new ArrayList<HashShuffleIntermediate>();
    for (PartitionAppenderMeta eachMeta : partitionAppenderMap.values()) {
      try {
        eachMeta.appender.close();
        HashShuffleIntermediate intermediate =
            new HashShuffleIntermediate(eachMeta.partId, eachMeta.appender.getOffset(),
                eachMeta.appender.getPages(),
                eachMeta.appender.getMergedTupleIndexes());
        intermediateEntries.add(intermediate);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      }
    }

    LOG.info("Close HashShuffleAppender:" + ebId + ", intermediates=" + intermediateEntries.size());

    return intermediateEntries;
  }

  public void finalizeTask(TaskAttemptId taskId) {
    synchronized (appenderMap) {
      Map<Integer, PartitionAppenderMeta> partitionAppenderMap =
        appenderMap.get(taskId.getTaskId().getExecutionBlockId());
      if (partitionAppenderMap == null) {
        return;
      }

      for (PartitionAppenderMeta eachAppender: partitionAppenderMap.values()) {
        eachAppender.appender.taskFinished(taskId);
      }
    }
  }

  public static class HashShuffleIntermediate {
    private int partId;

    private long volume;

    //[<page start offset,<task start, task end>>]
    private Collection<Pair<Long, Pair<Integer, Integer>>> failureTskTupleIndexes;

    //[<page start offset, length>]
    private List<Pair<Long, Integer>> pages = new ArrayList<Pair<Long, Integer>>();

    public HashShuffleIntermediate(int partId, long volume,
                                   List<Pair<Long, Integer>> pages,
                                   Collection<Pair<Long, Pair<Integer, Integer>>> failureTskTupleIndexes) {
      this.partId = partId;
      this.volume = volume;
      this.failureTskTupleIndexes = failureTskTupleIndexes;
      this.pages = pages;
    }

    public int getPartId() {
      return partId;
    }

    public long getVolume() {
      return volume;
    }

    public Collection<Pair<Long, Pair<Integer, Integer>>> getFailureTskTupleIndexes() {
      return failureTskTupleIndexes;
    }

    public List<Pair<Long, Integer>> getPages() {
      return pages;
    }
  }

  static class PartitionAppenderMeta {
    int partId;
    HashShuffleAppender appender;
    Path dataFile;

    public int getPartId() {
      return partId;
    }

    public HashShuffleAppender getAppender() {
      return appender;
    }

    public Path getDataFile() {
      return dataFile;
    }
  }
}
