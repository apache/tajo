/*
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

package org.apache.tajo.storage.jdbc;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.*;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.Pair;

import javax.annotation.Nullable;
import java.net.URI;
import java.sql.*;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.tajo.catalog.CatalogUtil.newSimpleDataType;

public abstract class JdbcMetadataProviderBase implements MetadataProvider {
  protected static final Log LOG = LogFactory.getLog(JdbcMetadataProviderBase.class);

  public static String [] GENERAL_TABLE_TYPES = new String [] {"TABLE"};

  protected final JdbcTablespace space;
  protected final String databaseName;

  protected final String jdbcUri;

  protected final Connection connection;

  public JdbcMetadataProviderBase(JdbcTablespace space, String dbName) {
    this.space = space;
    this.databaseName = dbName;

    ConnectionInfo connInfo = ConnectionInfo.fromURI(space.getUri());
    this.jdbcUri  = space.getUri().toASCIIString();

    try {
      Class.forName(getJdbcDriverName()).newInstance();
      LOG.info(getJdbcDriverName() + " is loaded...");
    } catch (Exception e) {
      throw new TajoInternalError(e);
    }

    try {
      connection = DriverManager.getConnection(jdbcUri, space.connProperties);
    } catch (SQLException e) {
      throw new TajoInternalError(e);
    }
  }

  @Override
  public String getTablespaceName() {
    return space.getName();
  }

  @Override
  public URI getTablespaceUri() {
    return space.getUri();
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public Collection<String> getSchemas() {
    return Collections.EMPTY_SET;
  }

  @Override
  public Collection<String> getTables(@Nullable String schemaPattern, @Nullable String tablePattern) {
    ResultSet res = null;
    List<String> tableNames = Lists.newArrayList();
    try {
      res = connection.getMetaData().getTables(databaseName, schemaPattern, tablePattern, GENERAL_TABLE_TYPES);
      while(res.next()) {
        tableNames.add(res.getString("TABLE_NAME"));
      }
    } catch (SQLException e) {
      throw new TajoInternalError(e);
    } finally {
      try {
        if (res != null) {
          res.close();
        }
      } catch (SQLException e) {
        LOG.warn(e);
      }
    }

    return tableNames;
  }

  private TypeDesc convertDataType(ResultSet res) throws SQLException {
    final int typeId = res.getInt("DATA_TYPE");

    switch (typeId ) {
    case Types.BOOLEAN:
      return new TypeDesc(newSimpleDataType(Type.BOOLEAN));

    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
      return new TypeDesc(newSimpleDataType(Type.INT4));

    case Types.DISTINCT: // sequence for postgresql
    case Types.BIGINT:
      return new TypeDesc(newSimpleDataType(Type.INT8));

    case Types.FLOAT:
      return new TypeDesc(newSimpleDataType(Type.FLOAT4));

    case Types.NUMERIC:
    case Types.DECIMAL:
    case Types.DOUBLE:
      return new TypeDesc(newSimpleDataType(Type.FLOAT8));

    case Types.DATE:
      return new TypeDesc(newSimpleDataType(Type.DATE));

    case Types.TIME:
      return new TypeDesc(newSimpleDataType(Type.TIME));

    case Types.TIMESTAMP:
      return new TypeDesc(newSimpleDataType(Type.TIMESTAMP));

    case Types.CHAR:
    case Types.NCHAR:
    case Types.VARCHAR:
    case Types.NVARCHAR:
    case Types.CLOB:
    case Types.NCLOB:
    case Types.LONGVARCHAR:
    case Types.LONGNVARCHAR:
      return new TypeDesc(newSimpleDataType(Type.TEXT));

    case Types.BINARY:
    case Types.VARBINARY:
    case Types.BLOB:
      return new TypeDesc(newSimpleDataType(Type.BLOB));

    default:
      throw SQLExceptionUtil.toSQLException(new UnsupportedDataTypeException(typeId + ""));
    }
  }

  @Override
  public TableDesc getTableDesc(String schemaName, String tableName) throws UndefinedTablespaceException {
    ResultSet resultForTable = null;
    ResultSet resultForColumns = null;
    try {

      // get table name
      resultForTable = connection.getMetaData().getTables(databaseName, schemaName, tableName, null);

      if (!resultForTable.next()) {
        throw new UndefinedTablespaceException(tableName);
      }
      final String name = resultForTable.getString("TABLE_NAME");

      // get columns
      resultForColumns = connection.getMetaData().getColumns(databaseName, schemaName, tableName, null);

      final List<Pair<Integer, Column>> columns = Lists.newArrayList();

      while(resultForColumns.next()) {
        final int ordinalPos = resultForColumns.getInt("ORDINAL_POSITION");
        final String qualifier = resultForColumns.getString("TABLE_NAME");
        final String columnName = resultForColumns.getString("COLUMN_NAME");
        final TypeDesc type = convertDataType(resultForColumns);
        final Column c = new Column(CatalogUtil.buildFQName(databaseName, qualifier, columnName), type);

        columns.add(new Pair<>(ordinalPos, c));
      }

      // sort columns in an order of ordinal position
      Collections.sort(columns, new Comparator<Pair<Integer, Column>>() {
        @Override
        public int compare(Pair<Integer, Column> o1, Pair<Integer, Column> o2) {
          return o1.getFirst() - o2.getFirst();
        }
      });

      // transform the pair list into collection for columns
      final Schema schema = new Schema(Collections2.transform(columns, new Function<Pair<Integer,Column>, Column>() {
        @Override
        public Column apply(@Nullable Pair<Integer, Column> columnPair) {
          return columnPair.getSecond();
        }
      }));


      // fill the table stats
      final TableStats stats = new TableStats();
      stats.setNumRows(-1); // unknown

      final TableDesc table = new TableDesc(
          CatalogUtil.buildFQName(databaseName, name),
          schema,
          new TableMeta("rowstore", new KeyValueSet()),
          space.getTableUri(databaseName, name)
      );
      table.setStats(stats);

      return table;

    } catch (SQLException e) {
      throw new TajoInternalError(e);
    } finally {
      try {
        if (resultForTable != null) {
          resultForTable.close();
        }

        if (resultForColumns != null) {
          resultForColumns.close();
        }

      } catch (SQLException e) {
        LOG.warn(e);
      }
    }
  }

  protected abstract String getJdbcDriverName();
}
