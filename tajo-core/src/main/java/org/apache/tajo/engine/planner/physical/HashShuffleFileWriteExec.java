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

package org.apache.tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.logical.ShuffleFileWriteNode;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.tuple.memory.RowBlock;
import org.apache.tajo.tuple.memory.RowWriter;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * <code>HashShuffleFileWriteExec</code> is a physical executor to store intermediate data into a number of
 * file outputs associated with shuffle keys. The file outputs are stored on local disks.
 */
public final class HashShuffleFileWriteExec extends UnaryPhysicalExec {
  private static final Log LOG = LogFactory.getLog(HashShuffleFileWriteExec.class);
  private static final int MAXIMUM_INITIAL_BUFFER_SIZE = StorageUnit.MB;
  private static final int MINIMUM_INITIAL_BUFFER_SIZE = 4 * StorageUnit.KB;
  // Buffer usage is greater than threshold, it will be flush to local storage
  private static final float BUFFER_THRESHOLD_FACTOR = 0.8f;

  private final ShuffleFileWriteNode plan;
  private final TableMeta meta;
  private final Partitioner partitioner;
  private final int numShuffleOutputs;
  private final int[] shuffleKeyIds;
  private final HashShuffleAppenderManager hashShuffleAppenderManager;
  private final int maxBufferSize;
  private final int bufferThreshold;
  private final int initialBufferSize;
  private final DataType[] dataTypes;

  private final Map<Integer, MemoryRowBlock> partitionMemoryMap;
  private long writtenBytes = 0;
  private long usedBufferSize = 0;
  private long totalBufferCapacity = 0;

  public HashShuffleFileWriteExec(TaskAttemptContext context,
                                  final ShuffleFileWriteNode plan, final PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    Preconditions.checkArgument(plan.hasShuffleKeys());
    this.plan = plan;
    if (plan.hasOptions()) {
      this.meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      this.meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }
    // about the shuffle
    this.numShuffleOutputs = this.plan.getNumOutputs();
    int i = 0;
    this.shuffleKeyIds = new int [this.plan.getShuffleKeys().length];
    for (Column key : this.plan.getShuffleKeys()) {
      shuffleKeyIds[i] = inSchema.getColumnId(key.getQualifiedName());
      i++;
    }
    this.partitioner = new HashPartitioner(shuffleKeyIds, numShuffleOutputs);
    this.hashShuffleAppenderManager = context.getHashShuffleAppenderManager();
    this.maxBufferSize = context.getQueryContext().getInt(SessionVars.HASH_SHUFFLE_BUFFER_SIZE) * StorageUnit.MB;
    this.bufferThreshold = (int) (maxBufferSize * BUFFER_THRESHOLD_FACTOR);
    this.dataTypes = SchemaUtil.toDataTypes(outSchema);

    if(numShuffleOutputs > 0){
      //calculate initial buffer by total partition. a buffer size will be 4Kb ~ 1MB
      this.initialBufferSize = Math.min(MAXIMUM_INITIAL_BUFFER_SIZE,
          Math.max(maxBufferSize / numShuffleOutputs, MINIMUM_INITIAL_BUFFER_SIZE));
    } else {
      this.initialBufferSize = MINIMUM_INITIAL_BUFFER_SIZE;
    }

    this.partitionMemoryMap = Maps.newHashMap();
  }

  @Override
  public void init() throws IOException {
    super.init();
  }

  @Override
  public Tuple next() throws IOException {
    try {
      Tuple tuple;
      int partId;
      long numRows = 0;
      while (!context.isStopped() && (tuple = child.next()) != null) {

        partId = partitioner.getPartition(tuple);
        MemoryRowBlock rowBlock = partitionMemoryMap.get(partId);
        if (rowBlock == null) {
          rowBlock = new MemoryRowBlock(dataTypes, initialBufferSize, true, plan.getStorageType());
          partitionMemoryMap.put(partId, rowBlock);
          totalBufferCapacity += rowBlock.capacity();
        }

        RowWriter writer = rowBlock.getWriter();
        long prevUsedMem = rowBlock.usedMem();
        totalBufferCapacity -= rowBlock.capacity();

        writer.addTuple(tuple);
        numRows++;

        totalBufferCapacity += rowBlock.capacity(); // calculate resizeable buffer capacity
        usedBufferSize += (rowBlock.usedMem() - prevUsedMem);

        // if total buffer capacity are required more than maxBufferSize,
        // all partitions are flushed and the buffers are released
        if (totalBufferCapacity > maxBufferSize) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Too low buffer usage. threshold: %s, total capacity: %s, used: %s",
                FileUtil.humanReadableByteCount(maxBufferSize, false),
                FileUtil.humanReadableByteCount(totalBufferCapacity, false),
                FileUtil.humanReadableByteCount(usedBufferSize, false)));
          }

          //flush and release buffer
          flushBuffer(partitionMemoryMap, true);
          writtenBytes += usedBufferSize;
          totalBufferCapacity = usedBufferSize = 0;

        } else if (usedBufferSize > bufferThreshold) {
          //flush and reuse buffer
          flushBuffer(partitionMemoryMap, false);
          writtenBytes += usedBufferSize;
          usedBufferSize = 0;
        }
      }

      // flush remaining buffers
      flushBuffer(partitionMemoryMap, true);

      writtenBytes += usedBufferSize;
      usedBufferSize = totalBufferCapacity = 0;
      TableStats aggregated = new TableStats();
      aggregated.setNumBytes(writtenBytes);
      aggregated.setNumRows(numRows);
      context.setResultStats(aggregated);

      return null;
    } catch (RuntimeException e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  /**
   * flush all buffer to local storage
   */
  private void flushBuffer(Map<Integer, MemoryRowBlock> partitionMemoryMap, boolean releaseBuffer)
      throws IOException, ExecutionException, InterruptedException {
    List<Future<MemoryRowBlock>> resultList = Lists.newArrayList();
    ArrayList<Integer> unusedBuffer = Lists.newArrayList();

    for (Map.Entry<Integer, MemoryRowBlock> entry : partitionMemoryMap.entrySet()) {
      int appendPartId = entry.getKey();

      MemoryRowBlock memoryRowBlock = entry.getValue();
      if (memoryRowBlock.getMemory().isReadable()) {
        //flush and release buffer
        resultList.add(hashShuffleAppenderManager.
            writePartitions(meta, outSchema, context.getTaskId(), appendPartId, memoryRowBlock, releaseBuffer));
      } else {
        if (releaseBuffer) {
          memoryRowBlock.release();
        } else {
          unusedBuffer.add(appendPartId);
        }
      }
    }

    // wait for flush to storage
    for (Future<MemoryRowBlock> future : resultList) {
      future.get();
    }

    if (releaseBuffer) {
      partitionMemoryMap.clear();
    } else {
      // release the unused partition
      for (Integer id : unusedBuffer) {
        MemoryRowBlock memoryRowBlock = partitionMemoryMap.remove(id);
        memoryRowBlock.release();
      }
    }
  }

  @Override
  public void rescan() throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void close() throws IOException{
    if (partitionMemoryMap.size() > 0) {
      for (RowBlock rowBlock : partitionMemoryMap.values()) {
        rowBlock.release();
      }
      partitionMemoryMap.clear();
    }

    progress = 1.0f;
    super.close();
  }
}
