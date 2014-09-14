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
import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.planner.physical.SeqScanExec;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NonForwardQueryResultScanner {
  private QueryId queryId;
  private String sessionId;
  private SeqScanExec scanExec;
  private TableDesc tableDesc;
  private RowStoreEncoder rowEncoder;
  private int maxRow;
  private int currentNumRows;

  public NonForwardQueryResultScanner(String sessionId, QueryId queryId, TableDesc tableDesc, int maxRow) {
    this.sessionId = sessionId;
    this.queryId = queryId;
    this.tableDesc = tableDesc;
    this.rowEncoder =  RowStoreUtil.createEncoder(tableDesc.getLogicalSchema());

    this.maxRow = maxRow;
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

  public List<ByteString> getNextRows(int fetchSize) throws IOException {
    List<ByteString> rows = new ArrayList<ByteString>();
    if (scanExec == null) {
      return rows;
    }
    int rowCount = 0;

    while (true) {
      Tuple tuple = scanExec.next();
      if (tuple == null) {
        break;
      }
      rows.add(ByteString.copyFrom((rowEncoder.toBytes(tuple))));
      rowCount++;
      currentNumRows++;
      if (rowCount >= fetchSize) {
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
