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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class TajoMetaDataResultSet extends TajoResultSetBase {
  private List<MetaDataTuple> values;

  public TajoMetaDataResultSet(Schema schema, List<MetaDataTuple> values) {
    super(null);
    init();
    this.schema = schema;
    setDataTuples(values);
  }

  public TajoMetaDataResultSet(List<String> columns, List<Type> types, List<MetaDataTuple> values) {
    super(null);
    init();
    schema = new Schema();

    int index = 0;
    if(columns != null) {
      for(String columnName: columns) {
        schema.addColumn(columnName, types.get(index++));
      }
    }
    setDataTuples(values);
  }

  protected void setDataTuples(List<MetaDataTuple> values) {
    this.values = values;
    this.totalRow = values == null ? 0 : values.size();
  }

  @Override
  protected Tuple nextTuple() throws IOException {
    if(curRow >= totalRow) {
      return null;
    }
    return values.get(curRow);
  }

  @Override
  public void close() throws SQLException {
  }

  @Override
  public String getString(int fieldId) throws SQLException {
    return cur.getText(fieldId - 1);
  }

  @Override
  public String getString(String name) throws SQLException {
    return cur.getText(findColumn(name));
  }
}
