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

package org.apache.tajo.thrift.client;

import org.apache.tajo.TajoConstants;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.thrift.TajoThriftUtil;
import org.apache.tajo.thrift.ThriftRowStoreDecoder;
import org.apache.tajo.thrift.generated.TQueryResult;
import org.apache.tajo.thrift.generated.TRowData;
import org.apache.tajo.thrift.generated.TTableDesc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class TajoThriftResultSet extends TajoResultSetBase {
  private TajoThriftClient tajoThriftClient;
  private String queryId;
  private TQueryResult queryResult;
  private List<TRowData> rowDatas;
  private Iterator<TRowData> rowIterator;
  private int fetchSize;
  private int maxRows;
  private long totalFetchRows;
  private ThriftRowStoreDecoder rowDecoder;
  private TTableDesc tableDesc;

  public TajoThriftResultSet(TajoThriftClient tajoThriftClient, String queryId, TQueryResult queryResult) {
    super(null);

    this.tajoThriftClient = tajoThriftClient;
    this.queryId = queryId;
    this.queryResult = queryResult;

    this.tableDesc = queryResult.getTableDesc();
    this.schema = TajoThriftUtil.convertSchema(tableDesc.getSchema());

    this.totalRows = tableDesc.getStats() != null ? tableDesc.getStats().getNumRows() : Integer.MAX_VALUE;
    if (this.totalRows == TajoConstants.UNKNOWN_ROW_NUMBER) {
      //Case of select * from table
      this.totalRows = Integer.MAX_VALUE;
    }
    this.rowDatas = queryResult.getRows();
    if (rowDatas != null) {
      this.rowIterator = rowDatas.iterator();
    }
    this.rowDecoder = new ThriftRowStoreDecoder(schema);
  }

  @Override
  protected Tuple nextTuple() throws IOException {
    if (maxRows > 0 && totalFetchRows >= maxRows) {
      return null;
    }

    if (rowIterator.hasNext()) {
      TRowData row = rowIterator.next();
      totalFetchRows++;
      return rowDecoder.toTuple(row);
    } else {
      queryResult = tajoThriftClient.getNextQueryResult(queryId, fetchSize);
      if (queryResult == null || queryResult.getRows() == null || queryResult.getRows().isEmpty()) {
        return null;
      } else {
        rowDatas = queryResult.getRows();
        rowIterator = rowDatas.iterator();
        return nextTuple();
      }
    }
  }

  @Override
  public void close() throws SQLException {
    if (rowDatas != null) {
      rowDatas = null;
      rowIterator = null;

      tajoThriftClient.closeQuery(queryId);
    }
  }

  @Override
  public void setFetchSize(int size) throws SQLException {
    this.fetchSize = size;
  }

  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public String getQueryId() {
    return queryId;
  }

  public long getTotalRow() {
    return totalRows;
  }

  public TQueryResult getQueryResult() {
    return queryResult;
  }


  public TTableDesc getTableDesc() {
    return tableDesc;
  }
}
