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

import io.netty.buffer.Unpooled;
import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.ipc.ClientProtos.SerializedResultSet;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.RowBlockReader;
import org.apache.tajo.tuple.memory.HeapRowBlockReader;
import org.apache.tajo.tuple.memory.HeapTuple;
import org.apache.tajo.tuple.memory.MemoryBlock;
import org.apache.tajo.tuple.memory.ResizableMemoryBlock;
import org.apache.tajo.util.CompressionUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class TajoMemoryResultSet extends TajoResultSetBase {
  private MemoryBlock memory;
  private RowBlockReader reader;
  private volatile boolean closed;


  public TajoMemoryResultSet(QueryId queryId, Schema schema, SerializedResultSet resultSet,
                             Map<String, String> clientSideSessionVars) {
    super(queryId, schema, clientSideSessionVars);
    if(resultSet != null && resultSet.getRows() > 0) {
      this.totalRow = resultSet.getRows();

      try {
        // decompress if a codec is specified
        if (resultSet.hasDecompressCodec()) {
          byte[] compressed = resultSet.getSerializedTuples().toByteArray();
          byte[] uncompressed = CompressionUtil.decompress(resultSet.getDecompressCodec(), compressed);
          memory = new ResizableMemoryBlock(Unpooled.wrappedBuffer(uncompressed));
        } else {
          memory = new ResizableMemoryBlock(resultSet.getSerializedTuples().asReadOnlyByteBuffer());
        }
      } catch (IOException e) {
        throw new TajoInternalError(e);
      }

      reader = new HeapRowBlockReader(memory, SchemaUtil.toDataTypes(schema), resultSet.getRows());
    } else {
      this.totalRow = 0;
      this.curRow = 0;
    }
  }

  @Override
  protected void init() {
    cur = null;
    curRow = 0;
    wasNull = false;
  }

  @Override
  public void close() throws SQLException {
    if (closed) {
      return;
    }

    closed = true;
    cur = null;
    curRow = -1;
    totalRow = 0;
    reader = null;
    if(memory != null) {
      memory.release();
      memory = null;
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    curRow = 0;
  }

  @Override
  protected Tuple nextTuple() throws IOException {
    if (curRow < totalRow) {

      HeapTuple heapTuple = new HeapTuple();
      if (reader.next(heapTuple)) {
        cur = heapTuple;
      } else {
        cur = null;
      }
      return cur;
    } else {
      return null;
    }
  }

  public boolean hasResult() {
    return totalRow > 0;
  }
}
