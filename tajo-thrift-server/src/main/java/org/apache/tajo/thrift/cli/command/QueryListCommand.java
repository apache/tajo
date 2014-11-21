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

import org.apache.commons.lang.StringUtils;
import org.apache.tajo.thrift.cli.TajoThriftCli.TajoThriftCliContext;
import org.apache.tajo.thrift.generated.TBriefQueryInfo;

import java.text.SimpleDateFormat;
import java.util.List;

public class QueryListCommand extends TajoThriftShellCommand {
  final static String DATE_FORMAT  = "yyyy-MM-dd HH:mm:ss";

  public QueryListCommand(TajoThriftCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\lq";
  }

  @Override
  public void invoke(String[] command) throws Exception {
    List<TBriefQueryInfo> queryList = client.getQueryList();
    SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
    StringBuilder builder = new StringBuilder();

    /* print title */
    builder.append(StringUtils.rightPad("QueryId", 21));
    builder.append(StringUtils.rightPad("State", 20));
    builder.append(StringUtils.rightPad("StartTime", 20));
    builder.append(StringUtils.rightPad("Duration", 20));
    builder.append(StringUtils.rightPad("Progress", 10));
    builder.append(StringUtils.rightPad("Query", 30)).append("\n");

    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 20), 21));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 19), 20));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 19), 20));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 19), 20));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 8), 10));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 29), 30)).append("\n");
    context.getOutput().write(builder.toString());

    builder = new StringBuilder();
    for (TBriefQueryInfo queryInfo : queryList) {
      long runTime = queryInfo.getFinishTime() > 0 ?
          queryInfo.getFinishTime() - queryInfo.getStartTime() : -1;

      builder.append(StringUtils.rightPad(queryInfo.getQueryId(), 21));
      builder.append(StringUtils.rightPad(queryInfo.getState(), 20));
      builder.append(StringUtils.rightPad(df.format(queryInfo.getStartTime()), 20));
      builder.append(StringUtils.rightPad(org.apache.tajo.util.StringUtils.formatTime(runTime), 20));
      builder.append(StringUtils.rightPad((int)(queryInfo.getProgress() * 100.0f)+ "%", 10));
      builder.append(StringUtils.abbreviate(queryInfo.getQuery(), 30)).append("\n");
    }
    context.getOutput().write(builder.toString());
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "list running queries";
  }
}
