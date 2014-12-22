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

package org.apache.tajo.thrift;

import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.ipc.ClientProtos.BriefQueryInfo;
import org.apache.tajo.thrift.generated.*;
import org.apache.tajo.thrift.generated.TajoThriftService.Client;
import org.apache.tajo.util.TajoIdUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class TajoThriftUtil {
  public static SessionIdProto makeSessionId(String sessionIdStr) {
    return SessionIdProto.newBuilder().setId(sessionIdStr).build();
  }

  public static boolean isQueryRunnning(QueryState state) {
    return state == QueryState.QUERY_NEW ||
        state == QueryState.QUERY_RUNNING ||
        state == QueryState.QUERY_MASTER_LAUNCHED ||
        state == QueryState.QUERY_MASTER_INIT ||
        state == QueryState.QUERY_NOT_ASSIGNED;
  }

  public static boolean isQueryRunnning(String stateName) {
    return stateName.equals(QueryState.QUERY_NEW.name()) ||
        stateName.equals(QueryState.QUERY_RUNNING.name())  ||
        stateName.equals(QueryState.QUERY_MASTER_LAUNCHED.name())  ||
        stateName.equals(QueryState.QUERY_MASTER_INIT.name())  ||
        stateName.equals(QueryState.QUERY_NOT_ASSIGNED.name()) ;
  }

  public static void close(Client client) {
    if (client == null) {
      return;
    }
    if (client.getOutputProtocol().getTransport().isOpen()) {
      client.getOutputProtocol().getTransport().close();
    }
    if (client.getInputProtocol().getTransport().isOpen()) {
      client.getInputProtocol().getTransport().close();
    }
  }

  public static TSchema convertSchema(Schema schema) {
    if (schema == null) {
      return null;
    }
    TSchema tSchema = new TSchema();

    List<TColumn> columns = new ArrayList<TColumn>();
    for (Column column: schema.getColumns()) {
      TColumn tColumn = new TColumn();
      tColumn.setName(column.getQualifiedName());
      tColumn.setSimpleName(column.getSimpleName());
      try {
        tColumn.setDataType(tajoTypeToThriftType(column.getDataType()));
        tColumn.setDataTypeName(column.getDataType().getType().name());
        tColumn.setSqlType(ResultSetUtil.tajoTypeToSqlType(column.getDataType()));
      } catch (SQLException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      tColumn.setSqlDataTypeName(ResultSetUtil.toSqlType(column.getDataType()));
      columns.add(tColumn);
    }
    tSchema.setColumns(columns);

    return tSchema;
  }

  public static TajoThriftDataType tajoTypeToThriftType(TajoDataTypes.DataType type) throws SQLException {
    switch (type.getType()) {
      case BOOLEAN:
        return TajoThriftDataType.BOOLEAN;
      case INT1:
        return TajoThriftDataType.INT1;
      case INT2:
        return TajoThriftDataType.INT2;
      case INT4:
        return TajoThriftDataType.INT4;
      case INT8:
        return TajoThriftDataType.INT8;
      case FLOAT4:
        return TajoThriftDataType.FLOAT4;
      case FLOAT8:
        return TajoThriftDataType.FLOAT8;
      case NUMERIC:
        return TajoThriftDataType.NUMERIC;
      case DATE:
        return TajoThriftDataType.DATE;
      case TIMESTAMP:
        return TajoThriftDataType.TIMESTAMP;
      case TIME:
        return TajoThriftDataType.TIME;
      case VARCHAR:
        return TajoThriftDataType.VARCHAR;
      case TEXT:
        return TajoThriftDataType.VARCHAR;
      default:
        throw new SQLException("Unrecognized column type: " + type);
    }
  }

  public static Type thriftTypeToTajoType(TajoThriftDataType type) throws SQLException {
    switch (type) {
      case BOOLEAN:
        return Type.BOOLEAN;
      case INT1:
        return Type.INT1;
      case INT2:
        return Type.INT2;
      case INT4:
        return Type.INT4;
      case INT8:
        return Type.INT8;
      case FLOAT4:
        return Type.FLOAT4;
      case FLOAT8:
        return Type.FLOAT8;
      case NUMERIC:
        return Type.NUMERIC;
      case DATE:
        return Type.DATE;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case TIME:
        return Type.TIME;
      case VARCHAR:
        return Type.VARCHAR;
      case TEXT:
        return Type.VARCHAR;
      default:
        throw new SQLException("Unrecognized column type: " + type);
    }
  }

  public static Schema convertSchema(TSchema tSchema) {
    if (tSchema == null) {
      return null;
    }
    Schema schema = new Schema();

    for (TColumn tColumn: tSchema.getColumns()) {
      Column column = null;
      try {
        column = new Column(tColumn.getName(), thriftTypeToTajoType(tColumn.getDataType()));
      } catch (SQLException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      schema.addColumn(column);
    }

    return schema;
  }

  public static TBriefQueryInfo convertQueryInfo(BriefQueryInfo queryInfo) {
    if (queryInfo == null) {
      return null;
    }
    TBriefQueryInfo tQueryInfo = new TBriefQueryInfo();

    tQueryInfo.setQueryId((new QueryId(queryInfo.getQueryId())).toString());
    tQueryInfo.setState(queryInfo.getState().name());
    tQueryInfo.setStartTime(queryInfo.getStartTime());
    tQueryInfo.setFinishTime(queryInfo.getFinishTime());
    tQueryInfo.setQuery(queryInfo.getQuery());
    tQueryInfo.setQueryMasterHost(queryInfo.getQueryMasterHost());
    tQueryInfo.setQueryMasterPort(queryInfo.getQueryMasterPort());
    tQueryInfo.setProgress(queryInfo.getProgress());

    return tQueryInfo;
  }

  public static TTableDesc convertTableDesc(TableDesc tableDesc) {
    if (tableDesc == null) {
      return null;
    }

    TTableDesc tTableDesc = new TTableDesc();

    tTableDesc.setTableName(tableDesc.getName());
    tTableDesc.setPath(tableDesc.getPath().toString());
    tTableDesc.setStoreType(tableDesc.getMeta().getStoreType().name());
    tTableDesc.setTableMeta(tableDesc.getMeta().toMap());
    tTableDesc.setSchema(convertSchema(tableDesc.getSchema()));
    tTableDesc.setStats(convertTableStats(tableDesc.getStats()));
    tTableDesc.setPartition(convertPartitionMethod(tableDesc.getPartitionMethod()));
    tTableDesc.setIsExternal(tableDesc.isExternal());

    return tTableDesc;
  }

  private static TPartitionMethod convertPartitionMethod(PartitionMethodDesc partitionMethod) {
    if (partitionMethod == null) {
      return null;
    }

    TPartitionMethod tPartitionMethod = new TPartitionMethod();

    tPartitionMethod.setTableName(partitionMethod.getTableName());
    tPartitionMethod.setPartitionType(partitionMethod.getPartitionType().name());
    tPartitionMethod.setExpression(partitionMethod.getExpression());
    tPartitionMethod.setExpressionSchema(convertSchema(partitionMethod.getExpressionSchema()));

    return tPartitionMethod;
  }

  private static TTableStats convertTableStats(TableStats stats) {
    if (stats == null) {
      return null;
    }
    TTableStats tStats = new TTableStats();

    tStats.setNumRows(stats.getNumRows());
    tStats.setNumBytes(stats.getNumBytes());
    tStats.setNumBlocks(stats.getNumBlocks());
    tStats.setNumShuffleOutputs(stats.getNumShuffleOutputs());
    tStats.setAvgRows(stats.getAvgRows());
    tStats.setReadBytes(stats.getReadBytes());

    return tStats;
  }
}
