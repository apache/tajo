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

import com.google.common.base.Function;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.StringUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

public class DDLBuilder {

  public static String buildDDLForExternalTable(TableDesc desc) {
    StringBuilder sb = new StringBuilder();

    sb.append("--\n")
      .append("-- Name: ").append(IdentifierUtil.denormalizeIdentifier(desc.getName())).append("; Type: TABLE;")
      .append(" Storage: ").append(desc.getMeta().getDataFormat());
    sb.append("\n-- Path: ").append(desc.getUri());
    sb.append("\n--\n");
    sb.append("CREATE EXTERNAL TABLE ").append(IdentifierUtil.denormalizeIdentifier(desc.getName()));
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
        .append("-- Name: ").append(IdentifierUtil.denormalizeIdentifier(desc.getName())).append("; Type: TABLE;")
        .append(" Storage: ").append(desc.getMeta().getDataFormat());
    sb.append("\n--\n");
    sb.append("CREATE TABLE ").append(IdentifierUtil.denormalizeIdentifier(desc.getName()));
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
        .append("-- Name: ").append(IdentifierUtil.denormalizeIdentifier(desc.getName())).append("; Type: INDEX;")
        .append(" Index Method: ").append(desc.getIndexMethod());
    sb.append("\n--\n");
    sb.append("CREATE INDEX ").append(IdentifierUtil.denormalizeIdentifier(desc.getName()));
    sb.append(" on ").append(IdentifierUtil.denormalizeIdentifier(desc.getTableName())).append(" ( ");

    for (SortSpec sortSpec : desc.getKeySortSpecs()) {
      sb.append(sortSpec.getSortKey().getQualifiedName()).append(" ");
      sb.append(sortSpec.isAscending() ? "asc" : "desc").append(" ");
      sb.append(sortSpec.isNullsFirst() ? "null first" : "null last").append(", ");
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

      sb.append(IdentifierUtil.denormalizeIdentifier(column.getSimpleName())).append(" ");
      TypeDesc typeDesc = column.getTypeDesc();
      sb.append(typeDesc);
    }
    sb.append(")");
  }

  private static void buildUsingClause(StringBuilder sb, TableMeta meta) {
    sb.append(" USING " +  CatalogUtil.getBackwardCompitableDataFormat(meta.getDataFormat()));
  }

  private static void buildWithClause(final StringBuilder sb, TableMeta meta) {
    KeyValueSet options = meta.getPropertySet();
    if (options != null && options.size() > 0) {

      sb.append(" WITH (");

      // sort table properties in an lexicographic order of the property keys.
      Entry<String, String> [] entries = meta.getPropertySet().getAllKeyValus().entrySet().toArray(
          new Entry[meta.getPropertySet().size()]);

      Arrays.sort(entries, new Comparator<Entry<String, String>>() {
        @Override
        public int compare(Entry<String, String> o1, Entry<String, String> o2) {
          return o1.getKey().compareTo(o2.getKey());
        }
      });

      // Join all properties by comma (',')
      sb.append(StringUtils.join(entries, ", ", new Function<Entry<String, String>, String>() {
        @Override
        public String apply(Entry<String, String> e) {
          return "'" + e.getKey() + "'='" + e.getValue() + "'";
        }
      }));

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
    sb.append("ALTER TABLE ").append(IdentifierUtil.denormalizeIdentifier(table.getName()))
      .append(" ADD IF NOT EXISTS PARTITION (");


    List<Column> colums = table.getPartitionMethod().getExpressionSchema().getAllColumns();

    String[] splitPartitionName = partition.getPartitionName().split(File.separator);
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
