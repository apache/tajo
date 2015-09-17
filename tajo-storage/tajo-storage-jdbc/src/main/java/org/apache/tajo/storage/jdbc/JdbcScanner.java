/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.jdbc;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.TimeDatum;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedDataTypeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.Properties;

public abstract class JdbcScanner implements Scanner {
  private static final Log LOG = LogFactory.getLog(JdbcScanner.class);

  protected final DatabaseMetaData dbMetaData;
  /** JDBC Connection Properties */
  protected final Properties connProperties;
  protected final String tableName;
  protected final Schema schema;
  protected final TableMeta tableMeta;
  protected final JdbcFragment fragment;
  protected final TableStats stats;
  protected final SQLBuilder builder;

  protected Column [] targets;
  protected EvalNode filter;
  protected Long limit;
  protected LogicalNode planPart;
  protected VTuple outTuple;
  protected String generatedSql;
  protected ResultSetIterator iter;

  protected int recordCount = 0;

  /**
   *
   * @param dbMetaData     DatabaseMetaData
   * @param connProperties JDBC Connection Properties
   * @param tableSchema    Table Schema
   * @param tableMeta      Table Properties
   * @param fragment       Fragment
   */
  public JdbcScanner(final DatabaseMetaData dbMetaData,
                     final Properties connProperties,
                     final Schema tableSchema,
                     final TableMeta tableMeta,
                     final JdbcFragment fragment) {

    Preconditions.checkNotNull(dbMetaData);
    Preconditions.checkNotNull(connProperties);
    Preconditions.checkNotNull(tableSchema);
    Preconditions.checkNotNull(tableMeta);
    Preconditions.checkNotNull(fragment);

    this.dbMetaData = dbMetaData;
    this.connProperties = connProperties;
    this.tableName = ConnectionInfo.fromURI(fragment.getUri()).tableName;
    this.schema = tableSchema;
    this.tableMeta = tableMeta;
    this.fragment = fragment;
    this.stats = new TableStats();
    builder = getSQLBuilder();
  }

  @Override
  public void init() throws IOException {
    if (targets == null) {
      targets = schema.toArray();
    }
    outTuple = new VTuple(targets.length);

    if (planPart == null) {
      generatedSql = builder.build(tableName, targets, filter, limit);
    } else {
      generatedSql = builder.build(planPart);
    }
  }

  @Override
  public Tuple next() throws IOException {
    if (iter == null) {
      iter = executeQueryAndGetIter();
    }

    if (iter.hasNext()) {
      return iter.next();
    } else {
      return null;
    }
  }

  @Override
  public void reset() throws IOException {
    if (iter != null) {
      iter.rewind();
    }
  }

  @Override
  public void close() throws IOException {
    if (iter != null) {
      iter.close();
    }
  }

  @Override
  public void pushOperators(LogicalNode planPart) {
    this.planPart = planPart;
  }


  @Override
  public boolean isProjectable() {
    return true;
  }

  @Override
  public void setTarget(Column [] targets) {
    this.targets = targets;
  }

  @Override
  public boolean isSelectable() {
    return true;
  }

  @Override
  public void setFilter(EvalNode filter) {
    this.filter = filter;
  }

  @Override
  public void setLimit(long num) {
    this.limit = num;
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public TableStats getInputStats() {
    return stats;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  protected SQLBuilder getSQLBuilder() {
    return new SQLBuilder(dbMetaData, getSQLExprBuilder());
  }
  protected SQLExpressionGenerator getSQLExprBuilder() {
    return new SQLExpressionGenerator(dbMetaData);
  }

  protected void convertTuple(ResultSet resultSet, VTuple tuple) {
    try {
      for (int column_idx = 0; column_idx < targets.length; column_idx++) {
        final Column c = targets[column_idx];
        final int resultIdx = column_idx + 1;

        switch (c.getDataType().getType()) {
        case INT1:
        case INT2:
          tuple.put(column_idx, DatumFactory.createInt2(resultSet.getShort(resultIdx)));
          break;
        case INT4:
          tuple.put(column_idx, DatumFactory.createInt4(resultSet.getInt(resultIdx)));
          break;
        case INT8:
          tuple.put(column_idx, DatumFactory.createInt8(resultSet.getLong(resultIdx)));
          break;
        case FLOAT4:
          tuple.put(column_idx, DatumFactory.createFloat4(resultSet.getFloat(resultIdx)));
          break;
        case FLOAT8:
          tuple.put(column_idx, DatumFactory.createFloat8(resultSet.getDouble(resultIdx)));
          break;
        case CHAR:
          tuple.put(column_idx, DatumFactory.createText(resultSet.getString(resultIdx)));
          break;
        case VARCHAR:
        case TEXT:
          // TODO - trim is unnecessary in many cases, so we can use it for certain cases
          tuple.put(column_idx, DatumFactory.createText(resultSet.getString(resultIdx).trim()));
          break;
        case DATE:
          final Date date = resultSet.getDate(resultIdx);
          tuple.put(column_idx, DatumFactory.createDate(1900 + date.getYear(), 1 + date.getMonth(), date.getDate()));
          break;
        case TIME:
          tuple.put(column_idx, new TimeDatum(resultSet.getTime(resultIdx).getTime() * 1000));
          break;
        case TIMESTAMP:
          tuple.put(column_idx,
              DatumFactory.createTimestmpDatumWithJavaMillis(resultSet.getTimestamp(resultIdx).getTime()));
          break;
        case BINARY:
        case VARBINARY:
        case BLOB:
          tuple.put(column_idx,
              DatumFactory.createBlob(resultSet.getBytes(resultIdx)));
          break;
        default:
          throw new TajoInternalError(new UnsupportedDataTypeException(c.getDataType().getType().name()));
        }
      }
    } catch (SQLException s) {
      throw new TajoInternalError(s);
    }
  }

  private ResultSetIterator executeQueryAndGetIter() {
    try {
      LOG.info("Generated SQL: " + generatedSql);
      Connection conn = DriverManager.getConnection(fragment.uri, connProperties);
      Statement statement = conn.createStatement();
      ResultSet resultset = statement.executeQuery(generatedSql);
      return new ResultSetIterator((resultset));
    } catch (SQLException s) {
      throw new TajoInternalError(s);
    }
  }

  public class ResultSetIterator implements Iterator<Tuple>, Closeable {

    private final ResultSet resultSet;

    private boolean didNext = false;
    private boolean hasNext = false;

    public ResultSetIterator(ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
      if (!didNext) {

        try {
          hasNext = resultSet.next();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

        didNext = true;
      }
      return hasNext;
    }

    @Override
    public Tuple next() {
      if (!didNext) {
        try {
          resultSet.next();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
      didNext = false;
      convertTuple(resultSet, outTuple);
      recordCount++;
      return outTuple;
    }

    @Override
    public void remove() {
      throw new TajoRuntimeException(new UnsupportedException());
    }

    public void rewind() {
      try {
        resultSet.isBeforeFirst();
      } catch (SQLException e) {
        throw new TajoInternalError(e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        resultSet.close();
      } catch (SQLException e) {
        LOG.warn(e);
      }

      if (stats != null) {
        stats.setNumRows(recordCount);
      }
    }
  }
}
