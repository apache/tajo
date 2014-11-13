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

package org.apache.tajo.master;

import com.google.protobuf.ByteString;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.QueryUnitId;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.physical.PhysicalPlanUtil;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.engine.planner.physical.SeqScanExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NonForwardQueryResultScanner {
  private static final int MAX_FILE_NUM_PER_SCAN = 100;

  private QueryId queryId;
  private String sessionId;
  private SeqScanExec scanExec;
  private TableDesc tableDesc;
  private RowStoreEncoder rowEncoder;
  private int maxRow;
  private int currentNumRows;
  private TaskAttemptContext taskContext;
  private TajoConf tajoConf;
  private ScanNode scanNode;

  private int currentFileIndex = 0;

  public NonForwardQueryResultScanner(TajoConf tajoConf, String sessionId,
                                      QueryId queryId,
                                      ScanNode scanNode,
                                      TableDesc tableDesc,
                                      int maxRow) throws IOException {
    this.tajoConf = tajoConf;
    this.sessionId = sessionId;
    this.queryId = queryId;
    this.scanNode = scanNode;
    this.tableDesc = tableDesc;
    this.maxRow = maxRow;

    this.rowEncoder =  RowStoreUtil.createEncoder(tableDesc.getLogicalSchema());
  }

  public void init() throws IOException {
    initSeqScanExec();
  }

  private void initSeqScanExec() throws IOException {
    FragmentProto[] fragments = PhysicalPlanUtil.getNonZeroLengthDataFiles(tajoConf, tableDesc,
        currentFileIndex, MAX_FILE_NUM_PER_SCAN);
    if (fragments != null && fragments.length > 0) {
      this.taskContext = new TaskAttemptContext(
          new QueryContext(tajoConf), null,
          new QueryUnitAttemptId(new QueryUnitId(new ExecutionBlockId(queryId, 1), 0), 0),
          fragments, null);

      try {
        // scanNode must be clone cause SeqScanExec change target in the case of a partitioned table.
        scanExec = new SeqScanExec(taskContext,
            StorageManager.getStorageManager(tajoConf), (ScanNode)scanNode.clone(), fragments);
      } catch (CloneNotSupportedException e) {
        throw new IOException(e.getMessage(), e);
      }
      scanExec.init();
      currentFileIndex += fragments.length;
    }
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setScanExec(SeqScanExec scanExec) {
    this.scanExec = scanExec;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public void close() throws Exception {
    if (scanExec != null) {
      scanExec.close();
      scanExec = null;
    }
  }

  public List<ByteString> getNextRows(int fetchRowNum) throws IOException {
    List<ByteString> rows = new ArrayList<ByteString>();
    if (scanExec == null) {
      return rows;
    }
    int rowCount = 0;

    while (true) {
      Tuple tuple = scanExec.next();
      if (tuple == null) {
        scanExec.close();
        scanExec = null;

        initSeqScanExec();
        if (scanExec != null) {
          tuple = scanExec.next();
        }
        if (tuple == null) {
          if (scanExec != null ) {
            scanExec.close();
            scanExec = null;
          }

          break;
        }
      }
      rows.add(ByteString.copyFrom((rowEncoder.toBytes(tuple))));
      rowCount++;
      currentNumRows++;
      if (rowCount >= fetchRowNum) {
        break;
      }

      if (currentNumRows >= maxRow) {
        scanExec.close();
        scanExec = null;
        break;
      }
    }

    return rows;
  }
}
