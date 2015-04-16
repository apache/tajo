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

package org.apache.tajo.jdbc;

import com.google.protobuf.ByteString;
import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoMemoryResultSet extends TajoResultSetBase {
  private List<ByteString> serializedTuples;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private RowStoreUtil.RowStoreDecoder decoder;

  public TajoMemoryResultSet(QueryId queryId, Schema schema, List<ByteString> serializedTuples, int maxRowNum,
                             Map<String, String> clientSideSessionVars) {
    super(queryId, schema, clientSideSessionVars);
    this.totalRow = maxRowNum;
    this.serializedTuples = serializedTuples;
    this.decoder = RowStoreUtil.createDecoder(schema);
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

    cur = null;
    curRow = -1;
    serializedTuples = null;
  }

  @Override
  public void beforeFirst() throws SQLException {
    curRow = 0;
  }

  @Override
  protected Tuple nextTuple() throws IOException {
    if (curRow < totalRow) {
      cur = decoder.toTuple(serializedTuples.get(curRow).toByteArray());
      return cur;
    } else {
      return null;
    }
  }

  public boolean hasResult() {
    return serializedTuples.size() > 0;
  }
}
