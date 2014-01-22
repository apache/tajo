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

package org.apache.tajo.cli;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.Specifier;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.util.FileUtil;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class DescTableCommand extends TajoShellCommand {
  public DescTableCommand(TajoClient client, PrintWriter sout) {
    super(client, sout);
  }

  @Override
  public String getCommand() {
    return "\\d";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 2) {
      TableDesc desc = client.getTableDesc(cmd[1]);
      if (desc == null) {
        sout.println("Did not find any relation named \"" + cmd[1] + "\"");
      } else {
        sout.println(toFormattedString(desc));
      }
    } else if (cmd.length == 1) {
      List<String> tableList = client.getTableList();
      if (tableList.size() == 0) {
        sout.println("No Relation Found");
      }
      for (String table : tableList) {
        sout.println(table);
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
    sb.append("table path: ").append(desc.getPath()).append("\n");
    sb.append("store type: ").append(desc.getMeta().getStoreType()).append("\n");
    if (desc.getStats() != null) {
      sb.append("number of rows: ").append(desc.getStats().getNumRows()).append("\n");
      sb.append("volume: ").append(
          FileUtil.humanReadableByteCount(desc.getStats().getNumBytes(),
              true)).append("\n");
    }
    sb.append("Options: \n");
    for(Map.Entry<String, String> entry : desc.getMeta().toMap().entrySet()){
      sb.append("\t").append("'").append(entry.getKey()).append("'").append("=")
          .append("'").append(entry.getValue()).append("'").append("\n");
    }
    sb.append("\n");
    sb.append("schema: \n");

    for(int i = 0; i < desc.getSchema().getColumnNum(); i++) {
      Column col = desc.getSchema().getColumn(i);
      sb.append(col.getColumnName()).append("\t").append(col.getDataType().getType());
      if (col.getDataType().hasLength()) {
        sb.append("(").append(col.getDataType().getLength()).append(")");
      }
      sb.append("\n");
    }

    sb.append("\n");
    sb.append("Partitions: \n");
    if (desc.getPartitions() != null) {
      sb.append("type:").append(desc.getPartitions().getPartitionsType().name()).append("\n");
      if (desc.getPartitions().getNumPartitions() > 0)
        sb.append("numbers:").append(desc.getPartitions().getNumPartitions()).append("\n");

      sb.append("columns:").append("\n");
      for(Column eachColumn: desc.getPartitions().getColumns()) {
        sb.append("  ");
        sb.append(eachColumn.getColumnName()).append("\t").append(eachColumn.getDataType().getType());
        if (eachColumn.getDataType().hasLength()) {
          sb.append("(").append(eachColumn.getDataType().getLength()).append(")");
        }
        sb.append("\n");
      }

      if (desc.getPartitions().getSpecifiers() != null) {
        sb.append("specifier:").append("\n");
        for(Specifier specifier :desc.getPartitions().getSpecifiers()) {
          sb.append("  ");
          sb.append("name:").append(specifier.getName());
          if (!specifier.getExpressions().equals("")) {
            sb.append(", expressions:").append(specifier.getExpressions());
          } else {
            if (desc.getPartitions().getPartitionsType().name().equals("RANGE"));
            sb.append(" expressions: MAXVALUE");
          }
          sb.append("\n");
        }
      }
    }

    return sb.toString();
  }
}
