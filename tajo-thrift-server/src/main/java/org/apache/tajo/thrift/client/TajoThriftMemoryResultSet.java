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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.thrift.ThriftRowStoreDecoder;
import org.apache.tajo.thrift.generated.TRowData;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoThriftMemoryResultSet extends TajoResultSetBase {
  private List<TRowData> rows;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private ThriftRowStoreDecoder decoder;
  private TajoThriftClient client;
  private String queryId;

  public TajoThriftMemoryResultSet(TajoThriftClient client, String queryId, Schema schema,
                                   List<TRowData> rows, int maxRowNum) {
    super(null);
    this.client = client;
    this.queryId = queryId;
    this.schema = schema;
    this.totalRows = maxRowNum;
    this.rows = rows;
    decoder = new ThriftRowStoreDecoder(schema);
    init();
  }

  @Override
  protected void init() {
    cur = null;
    curRow = 0;
  }

  @Override
  public synchronized void close() throws SQLException {
    if (closed.getAndSet(true)) {
      return;
    }

    client.closeQuery(queryId);
    cur = null;
    curRow = -1;
    rows.clear();
  }

  @Override
  public void beforeFirst() throws SQLException {
    curRow = 0;
  }

  @Override
  protected Tuple nextTuple() throws IOException {
    if (curRow < totalRows) {
      cur = decoder.toTuple(rows.get(curRow));
      return cur;
    } else {
      return null;
    }
  }

  public boolean hasResult() {
    return rows.size() > 0;
  }
}
