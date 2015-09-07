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
import com.google.protobuf.ByteString;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.physical.PartitionMergeScanExec;
import org.apache.tajo.engine.planner.physical.ScanExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.querymaster.Repartitioner;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NonForwardQueryResultFileScanner implements NonForwardQueryResultScanner {

  private QueryId queryId;
  private String sessionId;
  private ScanExec scanExec;
  private TableDesc tableDesc;
  private RowStoreEncoder rowEncoder;
  private int maxRow;
  private int currentNumRows;
  private TaskAttemptContext taskContext;
  private TajoConf tajoConf;
  private ScanNode scanNode;

  public NonForwardQueryResultFileScanner(TajoConf tajoConf, String sessionId, QueryId queryId, ScanNode scanNode,
      TableDesc tableDesc, int maxRow) throws IOException {
    this.tajoConf = tajoConf;
    this.sessionId = sessionId;
    this.queryId = queryId;
    this.scanNode = scanNode;
    this.tableDesc = tableDesc;
    this.maxRow = maxRow;
    this.rowEncoder = RowStoreUtil.createEncoder(tableDesc.getLogicalSchema());
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
      fragments.addAll(tablespace.getSplits(tableDesc.getName(), tableDesc, scanNode));
    }

    if (!fragments.isEmpty()) {
      FragmentProto[] fragmentProtos = FragmentConvertor.toFragmentProtoArray(fragments.toArray(new Fragment[]{}));
      this.taskContext = new TaskAttemptContext(
          new QueryContext(tajoConf), null,
          new TaskAttemptId(new TaskId(new ExecutionBlockId(queryId, 1), 0), 0),
          fragmentProtos, null);
      scanExec = new PartitionMergeScanExec(taskContext, scanNode, fragmentProtos);
      scanExec.init();
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

  public void close() throws Exception {
    if (scanExec != null) {
      scanExec.close();
      scanExec = null;
    }

    //remove temporal final output
    if (!tajoConf.getBoolVar(TajoConf.ConfVars.$DEBUG_ENABLED)) {
      Path temporalResultDir = TajoConf.getTemporalResultDir(tajoConf, queryId);
      if (tableDesc.getUri().equals(temporalResultDir.toUri())) {
        temporalResultDir.getParent().getFileSystem(tajoConf).delete(temporalResultDir, true);
      }
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
        break;
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

  @Override
  public Schema getLogicalSchema() {
    return tableDesc.getLogicalSchema();
  }
}
