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

package org.apache.tajo.thrift.cli.command;

import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.thrift.cli.TajoThriftCli.TajoThriftCliContext;
import org.apache.tajo.thrift.generated.TColumn;
import org.apache.tajo.thrift.generated.TPartitionMethod;
import org.apache.tajo.thrift.generated.TTableDesc;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Map;

public class DescTableCommand extends TajoThriftShellCommand {
  public DescTableCommand(TajoThriftCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\d";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 2) {
      String tableName = cmd[1];
      tableName = tableName.replace("\"", "");
      TTableDesc desc = client.getTableDesc(tableName);
      if (desc == null) {
        context.getOutput().println("Did not find any relation named \"" + tableName + "\"");
      } else {
        context.getOutput().println(toFormattedString(desc));
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

  protected String toFormattedString(TTableDesc desc) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ntable name: ").append(desc.getTableName()).append("\n");
    sb.append("table path: ").append(desc.getPath()).append("\n");
    sb.append("store type: ").append(desc.getStoreType()).append("\n");
    if (desc.getStats() != null) {
      sb.append("number of rows: ").append(desc.getStats().getNumRows()).append("\n");
      sb.append("volume: ").append(
          FileUtil.humanReadableByteCount(desc.getStats().getNumBytes(),
              true)).append("\n");
    }
    sb.append("Options: \n");
    for(Map.Entry<String, String> entry : desc.getTableMeta().entrySet()){

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

    for(int i = 0; i < desc.getSchema().getColumns().size(); i++) {
      TColumn col = desc.getSchema().getColumns().get(i);
      sb.append(col.getName()).append("\t").append(col.getDataType());
      sb.append("\n");
    }

    sb.append("\n");
    if (desc.getPartition() != null) {
      TPartitionMethod partition = desc.getPartition();
      sb.append("Partitions: \n");

      sb.append("type:").append(partition.getPartitionType()).append("\n");

      sb.append("columns:").append(":");
      sb.append(TUtil.arrayToString(partition.getExpressionSchema().getColumns().toArray()));
    }

    return sb.toString();
  }
}
