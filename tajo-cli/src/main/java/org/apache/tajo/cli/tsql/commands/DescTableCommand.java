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

package org.apache.tajo.cli.tsql.commands;

import jline.console.completer.ArgumentCompleter;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SortSpecProto;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.StringUtils;

import java.util.List;
import java.util.Map;

public class DescTableCommand extends TajoShellCommand {
  public DescTableCommand(TajoCli.TajoCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\d";
  }

  @Override
  public void invoke(String[] cmd) throws TajoException {
    if (cmd.length >= 2) {
      StringBuilder tableNameMaker = new StringBuilder();
      for (int i = 1; i < cmd.length; i++) {
        if (i != 1) {
          tableNameMaker.append(" ");
        }
        tableNameMaker.append(cmd[i]);
      }
      String tableName = tableNameMaker.toString().replace("\"", "");
      TableDesc desc = client.getTableDesc(tableName);
      if (desc == null) {
        context.getOutput().println("Did not find any relation named \"" + tableName + "\"");
      } else {
        context.getOutput().println(toFormattedString(desc));
        // If there exists any indexes for the table, print index information
        if (client.hasIndexes(tableName)) {
          StringBuilder sb = new StringBuilder();
          sb.append("Indexes:\n");
          for (IndexDescProto index : client.getIndexes(tableName)) {
            sb.append("\"").append(index.getIndexName()).append("\" ");
            sb.append(index.getIndexMethod()).append(" (");
            for (SortSpecProto key : index.getKeySortSpecsList()) {
              sb.append(CatalogUtil.extractSimpleName(key.getColumn().getName()));
              sb.append(key.getAscending() ? " ASC" : " DESC");
              sb.append(key.getNullFirst() ? " NULLS FIRST, " : " NULLS LAST, ");
            }
            sb.delete(sb.length()-2, sb.length()-1).append(")\n");
          }
          context.getOutput().println(sb.toString());
        }
      }
    } else if (cmd.length == 1) {
      List<String> tableList = client.getTableList(null);
      if (tableList.size() == 0) {
        context.getOutput().println("No Relation Found");
      }
      for (String table : tableList) {
        context.getOutput().println(table);
      }
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public String getUsage() {
    return "[table_name]";
  }

  @Override
  public String getDescription() {
    return "show table description";
  }

  protected String toFormattedString(TableDesc desc) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ntable name: ").append(desc.getName()).append("\n");
    sb.append("table uri: ").append(desc.getUri()).append("\n");
    sb.append("store type: ").append(desc.getMeta().getDataFormat()).append("\n");
    if (desc.getStats() != null) {

      long row = desc.getStats().getNumRows();
      String rowText = row == TajoConstants.UNKNOWN_ROW_NUMBER ? "unknown" : row + "";
      sb.append("number of rows: ").append(rowText).append("\n");
      sb.append("volume: ").append(
          FileUtil.humanReadableByteCount(desc.getStats().getNumBytes(),
              true)).append("\n");
    }
    sb.append("Options: \n");
    for(Map.Entry<String, String> entry : desc.getMeta().toMap().entrySet()){

      /*
      *  Checks whether the character is ASCII 7 bit printable.
      *  For example, a printable unicode '\u007c' become the character ‘|’.
      *
      *  Control-chars : ctrl-a(\u0001), tab(\u0009) ..
      *  Printable-chars : '|'(\u007c), ','(\u002c) ..
      * */

      String value = entry.getValue();
      String unescaped = StringEscapeUtils.unescapeJava(value);
      if (unescaped.length() == 1 && CharUtils.isAsciiPrintable(unescaped.charAt(0))) {
        value = unescaped;
      }
      sb.append("\t").append("'").append(entry.getKey()).append("'").append("=")
          .append("'").append(value).append("'").append("\n");
    }
    sb.append("\n");
    sb.append("schema: \n");

    for(int i = 0; i < desc.getSchema().size(); i++) {
      Column col = desc.getSchema().getColumn(i);
      sb.append(col.getSimpleName()).append("\t").append(col.getTypeDesc());
      sb.append("\n");
    }

    sb.append("\n");
    if (desc.getPartitionMethod() != null) {
      PartitionMethodDesc partition = desc.getPartitionMethod();
      sb.append("Partitions: \n");

      sb.append("type:").append(partition.getPartitionType().name()).append("\n");

      sb.append("columns:").append(":");
      sb.append(StringUtils.join(partition.getExpressionSchema().toArray()));
    }

    return sb.toString();
  }

  @Override
  public ArgumentCompleter getArgumentCompleter() {
    return new ArgumentCompleter(
        new StringsCompleter(getCommand()),
        new TableNameCompleter(),
        NullCompleter.INSTANCE);
  }
}
