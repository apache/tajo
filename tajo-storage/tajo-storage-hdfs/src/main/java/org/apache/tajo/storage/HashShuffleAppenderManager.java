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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class HashShuffleAppenderManager {
  private static final Log LOG = LogFactory.getLog(HashShuffleAppenderManager.class);

  private ConcurrentMap<ExecutionBlockId, Map<Integer, PartitionAppenderMeta>> appenderMap = Maps.newConcurrentMap();
  private ConcurrentMap<Integer, ExecutorService> executors = Maps.newConcurrentMap(); // for parallel writing
  private List<String> temporalPaths = Lists.newArrayList();

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
    pageSize = systemConf.getIntVar(ConfVars.SHUFFLE_HASH_APPENDER_PAGE_VOLUME) * StorageUnit.MB;

    Iterable<Path> allLocalPath = lDirAllocator.getAllLocalPathsToRead(".", systemConf);

    //add async hash shuffle writer
    for (Path path : allLocalPath) {
      temporalPaths.add(localFS.makeQualified(path).toString());
      executors.put(temporalPaths.size() - 1, Executors.newSingleThreadExecutor());
    }
  }

  protected int getVolumeId(Path path) {
    int i = 0;
    for (String rootPath : temporalPaths) {
      if (path.toString().startsWith(rootPath)) {
        break;
      }
      i++;
    }
    Preconditions.checkPositionIndex(i, temporalPaths.size() - 1);
    return i;
  }

  public synchronized HashShuffleAppenderWrapper getAppender(MemoryRowBlock memoryRowBlock, ExecutionBlockId ebId, 
                                                             int partId, TableMeta meta, Schema outSchema) 
      throws IOException {
    
    Map<Integer, PartitionAppenderMeta> partitionAppenderMap = appenderMap.get(ebId);

    if (partitionAppenderMap == null) {
      partitionAppenderMap = new ConcurrentHashMap<>();
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

      DirectRawFileWriter appender =
          new DirectRawFileWriter(systemConf, null, outSchema, meta, dataFile, memoryRowBlock);
      appender.enableStats();
      appender.init();

      partitionAppenderMeta = new PartitionAppenderMeta();
      partitionAppenderMeta.partId = partId;
      partitionAppenderMeta.dataFile = dataFile;
      partitionAppenderMeta.appender =
          new HashShuffleAppenderWrapper(ebId, partId, pageSize, appender, getVolumeId(dataFile));
      partitionAppenderMeta.appender.init();
      partitionAppenderMap.put(partId, partitionAppenderMeta);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Create Hash shuffle file(partId=" + partId + "): " + dataFile);
      }
    }

    return partitionAppenderMeta.appender;
  }

  public static int getPartParentId(int partId, TajoConf tajoConf) {
    return partId % tajoConf.getIntVar(ConfVars.SHUFFLE_HASH_PARENT_DIRS);
  }

  private Path getDataFile(ExecutionBlockId ebId, int partId) throws IOException {
    try {
      // the base dir for an output dir
      String executionBlockBaseDir = ebId.getQueryId().toString() + "/output" + "/" + ebId.getId() + "/hash-shuffle";
      Path baseDirPath = lDirAllocator.getLocalPathForWrite(executionBlockBaseDir, systemConf);
      //LOG.info(ebId + "'s basedir is created (" + baseDirPath + ")");

      // If EB has many partition, too many shuffle file are in single directory.
      return localFS.makeQualified(
          StorageUtil.concatPath(baseDirPath, "" + getPartParentId(partId, systemConf), "" + partId));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public List<HashShuffleIntermediate> close(ExecutionBlockId ebId) throws IOException {
    Map<Integer, PartitionAppenderMeta> partitionAppenderMap = appenderMap.remove(ebId);

    if (partitionAppenderMap == null) {
      LOG.info("Close HashShuffleAppenderWrapper:" + ebId + ", not a hash shuffle");
      return null;
    }

    // Send Intermediate data to QueryMaster.
    List<HashShuffleIntermediate> intermediateEntries = new ArrayList<>();
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

    LOG.info("Close HashShuffleAppenderWrapper:" + ebId + ", intermediates=" + intermediateEntries.size());

    return intermediateEntries;
  }

  public void finalizeTask(TaskAttemptId taskId) {
    Map<Integer, PartitionAppenderMeta> partitionAppenderMap =
        appenderMap.get(taskId.getTaskId().getExecutionBlockId());
    if (partitionAppenderMap == null) {
      return;
    }

    for (PartitionAppenderMeta eachAppender: partitionAppenderMap.values()) {
      eachAppender.appender.taskFinished(taskId);
    }
  }

  /**
   * Asynchronously write partitions.
   */
  public Future<MemoryRowBlock> writePartitions(TableMeta meta, Schema schema, final TaskAttemptId taskId, int partId,
                                                final MemoryRowBlock rowBlock,
                                                final boolean release) throws IOException {

    HashShuffleAppenderWrapper appender =
        getAppender(rowBlock, taskId.getTaskId().getExecutionBlockId(), partId, meta, schema);
    ExecutorService executor = executors.get(appender.getVolumeId());
    return executor.submit(() -> {
      appender.writeRowBlock(taskId, rowBlock);

      if (release) rowBlock.release();
      else rowBlock.clear();

      return rowBlock;
    });
  }

  public void shutdown() {
    for (ExecutorService service : executors.values()) {
      service.shutdownNow();
    }
  }

  public static class HashShuffleIntermediate {
    private int partId;

    private long volume;

    //[<page start offset,<task start, task end>>]
    private Collection<Pair<Long, Pair<Integer, Integer>>> failureTskTupleIndexes;

    //[<page start offset, length>]
    private List<Pair<Long, Integer>> pages = new ArrayList<>();

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
    HashShuffleAppenderWrapper appender;
    Path dataFile;

    public int getPartId() {
      return partId;
    }

    public HashShuffleAppenderWrapper getAppender() {
      return appender;
    }

    public Path getDataFile() {
      return dataFile;
    }
  }
}
