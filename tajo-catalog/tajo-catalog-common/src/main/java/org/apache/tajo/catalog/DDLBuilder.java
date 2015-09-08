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

package org.apache.tajo.catalog;

import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.util.KeyValueSet;

import java.util.List;
import java.util.Map;

public class DDLBuilder {

  public static String buildDDLForExternalTable(TableDesc desc) {
    StringBuilder sb = new StringBuilder();

    sb.append("--\n")
      .append("-- Name: ").append(CatalogUtil.denormalizeIdentifier(desc.getName())).append("; Type: TABLE;")
      .append(" Storage: ").append(desc.getMeta().getStoreType());
    sb.append("\n-- Path: ").append(desc.getUri());
    sb.append("\n--\n");
    sb.append("CREATE EXTERNAL TABLE ").append(CatalogUtil.denormalizeIdentifier(desc.getName()));
    buildSchema(sb, desc.getSchema());
    buildUsingClause(sb, desc.getMeta());
    buildWithClause(sb, desc.getMeta());

    if (desc.hasPartition()) {
      buildPartitionClause(sb, desc);
    }

    buildLocationClause(sb, desc);

    sb.append(";");
    return sb.toString();
  }

  public static String buildDDLForBaseTable(TableDesc desc) {
    StringBuilder sb = new StringBuilder();

    sb.append("--\n")
        .append("-- Name: ").append(CatalogUtil.denormalizeIdentifier(desc.getName())).append("; Type: TABLE;")
        .append(" Storage: ").append(desc.getMeta().getStoreType());
    sb.append("\n--\n");
    sb.append("CREATE TABLE ").append(CatalogUtil.denormalizeIdentifier(desc.getName()));
    buildSchema(sb, desc.getSchema());
    buildUsingClause(sb, desc.getMeta());
    buildWithClause(sb, desc.getMeta());

    if (desc.hasPartition()) {
      buildPartitionClause(sb, desc);
    }

    sb.append(";");
    return sb.toString();
  }

  public static String buildDDLForIndex(IndexDesc desc) {
    StringBuilder sb = new StringBuilder();

    sb.append("--\n")
        .append("-- Name: ").append(CatalogUtil.denormalizeIdentifier(desc.getName())).append("; Type: INDEX;")
        .append(" Index Method: ").append(desc.getIndexMethod());
    sb.append("\n--\n");
    sb.append("CREATE INDEX ").append(CatalogUtil.denormalizeIdentifier(desc.getName()));
    sb.append(" on ").append(CatalogUtil.denormalizeIdentifier(desc.getTableName())).append(" ( ");

    for (SortSpec sortSpec : desc.getKeySortSpecs()) {
      sb.append(sortSpec.getSortKey().getQualifiedName()).append(" ");
      sb.append(sortSpec.isAscending() ? "asc" : "desc").append(" ");
      sb.append(sortSpec.isNullFirst() ? "null first" : "null last").append(", ");
    }
    sb.replace(sb.length() - 2, sb.length() - 1, " )");

    sb.append(" location '").append(desc.getIndexPath()).append("';");

    return sb.toString();
  }

  public static void buildSchema(StringBuilder sb, Schema schema) {
    boolean first = true;

    sb.append(" (");
    for (Column column : schema.toArray()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }

      sb.append(CatalogUtil.denormalizeIdentifier(column.getSimpleName())).append(" ");
      TypeDesc typeDesc = column.getTypeDesc();
      sb.append(typeDesc);
    }
    sb.append(")");
  }

  private static void buildUsingClause(StringBuilder sb, TableMeta meta) {
    sb.append(" USING " +  CatalogUtil.getBackwardCompitablityStoreType(meta.getStoreType()));
  }

  private static void buildWithClause(StringBuilder sb, TableMeta meta) {
    KeyValueSet options = meta.getOptions();
    if (options != null && options.size() > 0) {
      boolean first = true;
      sb.append(" WITH (");
      for (Map.Entry<String, String> entry : meta.getOptions().getAllKeyValus().entrySet()) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append("'").append(entry.getKey()).append("'='").append(entry.getValue()).append("'");
      }
      sb.append(")");
    }
  }

  private static void buildLocationClause(StringBuilder sb, TableDesc desc) {
    sb.append(" LOCATION '").append(desc.getUri()).append("'");
  }

  private static void buildPartitionClause(StringBuilder sb, TableDesc desc) {
    PartitionMethodDesc partitionDesc = desc.getPartitionMethod();

    sb.append(" PARTITION BY ");
    sb.append(partitionDesc.getPartitionType().name());

    // columns
    sb.append("(");
    String prefix = "";
    for (Column column : partitionDesc.getExpressionSchema().toArray()) {
      sb.append(prefix).append(CatalogUtil.columnToDDLString(column));
      prefix = ", ";
    }
    sb.append(")");
  }

  /**
   * Build alter table add partition statement
   *
   * @param table TableDesc to be build
   * @param partition PartitionDescProto to be build
   * @return
   */
  public static String buildDDLForAddPartition(TableDesc table, PartitionDescProto partition) {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ").append(CatalogUtil.denormalizeIdentifier(table.getName()))
      .append(" ADD IF NOT EXISTS PARTITION (");


    List<Column> colums = table.getPartitionMethod().getExpressionSchema().getAllColumns();

    String[] splitPartitionName = partition.getPartitionName().split("/");

    for(int i = 0; i < splitPartitionName.length; i++) {
      String[] partitionColumnValue = splitPartitionName[i].split("=");
      if (i > 0) {
        sb.append(",");
      }

      switch (colums.get(i).getDataType().getType()) {
        case TEXT:
        case TIME:
        case TIMESTAMP:
        case DATE:
          sb.append(partitionColumnValue[0]).append("='").append(partitionColumnValue[1]).append("'");
          break;
        default:
          sb.append(partitionColumnValue[0]).append("=").append(partitionColumnValue[1]);
          break;
      }
    }
    sb.append(") LOCATION '").append(partition.getPath()).append("';\n");
    return sb.toString();
  }
}
