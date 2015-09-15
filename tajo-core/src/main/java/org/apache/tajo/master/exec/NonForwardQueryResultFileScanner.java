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

package org.apache.tajo.master.exec;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.TajoProtos.CodecType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.physical.PartitionMergeScanExec;
import org.apache.tajo.engine.planner.physical.ScanExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.ipc.ClientProtos.SerializedResultSet;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.querymaster.Repartitioner;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.tuple.memory.MemoryBlock;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.util.CompressionUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class NonForwardQueryResultFileScanner implements NonForwardQueryResultScanner {
  private final static Log LOG = LogFactory.getLog(NonForwardQueryResultFileScanner.class);

  private QueryId queryId;
  private String sessionId;
  private ScanExec scanExec;
  private TableDesc tableDesc;
  private RowStoreEncoder rowEncoder;
  private int maxRow;
  private boolean eof;
  private volatile long totalRows;
  private volatile int currentNumRows;
  private volatile boolean isStopped;
  private TaskAttemptContext taskContext;
  private TajoConf tajoConf;
  private ScanNode scanNode;
  private CodecType codecType;
  private ExecutorService executor;
  private MemoryRowBlock rowBlock;
  private Future<MemoryRowBlock> nextFetch;

  public NonForwardQueryResultFileScanner(TajoConf tajoConf, String sessionId, QueryId queryId, ScanNode scanNode,
                                          TableDesc tableDesc, int maxRow) throws IOException {
    this(tajoConf, sessionId, queryId, scanNode, tableDesc, maxRow, null);
  }

  public NonForwardQueryResultFileScanner(TajoConf tajoConf, String sessionId, QueryId queryId, ScanNode scanNode,
      TableDesc tableDesc, int maxRow, CodecType codecType) throws IOException {
    this.tajoConf = tajoConf;
    this.sessionId = sessionId;
    this.queryId = queryId;
    this.scanNode = scanNode;
    this.tableDesc = tableDesc;
    this.maxRow = maxRow;
    this.rowEncoder = RowStoreUtil.createEncoder(tableDesc.getLogicalSchema());
    this.codecType = codecType;
  }

  public void init() throws IOException, TajoException {
    initSeqScanExec();
  }

  private void initSeqScanExec() throws IOException, TajoException {
    Tablespace tablespace = TablespaceManager.get(tableDesc.getUri()).get();

    List<Fragment> fragments = Lists.newArrayList();
    if (tableDesc.hasPartition()) {
      FileTablespace fileTablespace = TUtil.checkTypeAndGet(tablespace, FileTablespace.class);
      fragments.addAll(Repartitioner.getFragmentsFromPartitionedTable(fileTablespace, scanNode, tableDesc));
    } else {
      fragments.addAll(tablespace.getSplits(tableDesc.getName(), tableDesc, scanNode.getQual()));
    }

    if (!fragments.isEmpty()) {
      FragmentProto[] fragmentProtos = FragmentConvertor.toFragmentProtoArray(fragments.toArray(new Fragment[]{}));
      this.taskContext = new TaskAttemptContext(
          new QueryContext(tajoConf), null,
          new TaskAttemptId(new TaskId(new ExecutionBlockId(queryId, 1), 0), 0),
          fragmentProtos, null);
      this.scanExec = new PartitionMergeScanExec(taskContext, scanNode, fragmentProtos);
      this.scanExec.init();
    } else {
      close();
    }
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }

  @Override
  public int getCurrentRowNumber() {
    return currentNumRows;
  }

  public void close() throws IOException {
    if(isStopped) {
      return;
    }

    isStopped = true;
    if (scanExec != null) {
      scanExec.close();
      scanExec = null;
    }
    
    if(rowBlock != null) {
      rowBlock.release();
      rowBlock = null;
    }

    if(executor != null) {
      executor.shutdown();
    }

    //remove temporal final output
    if (!tajoConf.getBoolVar(TajoConf.ConfVars.$DEBUG_ENABLED)) {
      Path temporalResultDir = TajoConf.getTemporalResultDir(tajoConf, queryId);
      if (tableDesc.getUri().equals(temporalResultDir.toUri())) {
        temporalResultDir.getFileSystem(tajoConf).delete(temporalResultDir.getParent(), true);
      }
    }


    LOG.info(String.format("\"Sent result to client for %s, queryId: %s %s rows: %d",
        sessionId, queryId,
        codecType != null ? ", compression: " + codecType : "",
        totalRows
    ));
  }

  public List<ByteString> getNextRows(int fetchRowNum) throws IOException {
    List<ByteString> rows = new ArrayList<>();
    if (scanExec == null) {
      return rows;
    }
    int rowCount = 0;
    while (!eof) {
      Tuple tuple = scanExec.next();
      if (tuple == null) {
        eof = true;
        break;
      }

      rows.add(ByteString.copyFrom((rowEncoder.toBytes(tuple))));
      rowCount++;
      currentNumRows++;
      if (rowCount >= fetchRowNum) {
        break;
      }
      if (currentNumRows >= maxRow) {
        eof = true;
        break;
      }
    }

    if(eof) {
      close();
    }
    return rows;
  }

  @Override
  public SerializedResultSet nextRowBlock(int fetchRowNum) throws IOException {
    try {
      final SerializedResultSet.Builder resultSetBuilder = SerializedResultSet.newBuilder();
      resultSetBuilder.setSchema(getLogicalSchema().getProto());
      resultSetBuilder.setRows(0);

      if (isStopped) return resultSetBuilder.build();

      if (nextFetch == null) {
        nextFetch = fetchNextRowBlock(fetchRowNum);
      }

      MemoryRowBlock rowBlock = nextFetch.get();

      if (rowBlock.rows() > 0) {
        resultSetBuilder.setRows(rowBlock.rows());
        MemoryBlock memoryBlock = rowBlock.getMemory();

        if (codecType != null) {
          byte[] uncompressedBytes = new byte[memoryBlock.readableBytes()];
          memoryBlock.getBuffer().getBytes(0, uncompressedBytes);

          byte[] compressedBytes = CompressionUtil.compress(codecType, uncompressedBytes);
          resultSetBuilder.setDecompressedLength(uncompressedBytes.length);
          resultSetBuilder.setDecompressCodec(codecType);
          resultSetBuilder.setSerializedTuples(ByteString.copyFrom(compressedBytes));
        } else {
          ByteBuffer uncompressed = memoryBlock.getBuffer().nioBuffer(0, memoryBlock.readableBytes());
          resultSetBuilder.setDecompressedLength(uncompressed.remaining());
          resultSetBuilder.setSerializedTuples(ByteString.copyFrom(uncompressed));
        }
      }

      // pre-fetch
      if (!eof) {
        nextFetch = fetchNextRowBlock(fetchRowNum);
      } else {
        close();
      }
      return resultSetBuilder.build();
    } catch (Throwable t) {
      throw new IOException(t.getMessage(), t);
    }
  }

  /**
   * Asynchronously fetch final output data
   */
  private Future<MemoryRowBlock> fetchNextRowBlock(final int fetchRowNum) throws IOException {
    final SettableFuture<MemoryRowBlock> future = SettableFuture.create();
    if (rowBlock == null) {
      rowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(tableDesc.getLogicalSchema()));
    }

    if (scanExec == null) {
      rowBlock.clear();
      future.set(rowBlock);
      return future;
    }

    if (executor == null) {
      executor = Executors.newSingleThreadExecutor();
    }

    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          rowBlock.clear();
          int endRow = currentNumRows + fetchRowNum;
          while (currentNumRows < endRow) {
            Tuple tuple = scanExec.next();
            if (tuple == null) {
              eof = true;
              break;
            } else {
              rowBlock.getWriter().addTuple(tuple);
              currentNumRows++;
              if (currentNumRows >= maxRow) {
                eof = true;
                break;
              }
            }
          }

          if (rowBlock.rows() > 0) {
            totalRows += rowBlock.rows();
          }

          future.set(rowBlock);
        } catch (IOException e) {
          future.setException(e);
        }
      }
    });
    return future;
  }

  @Override
  public Schema getLogicalSchema() {
    return tableDesc.getLogicalSchema();
  }
}
