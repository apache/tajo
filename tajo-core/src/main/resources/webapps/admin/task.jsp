<%
    /*
    * Licensed to the Apache Software Foundation (ASF) under one
    * or more contributor license agreements. See the NOTICE file
    * distributed with this work for additional information
    * regarding copyright ownership. The ASF licenses this file
    * to you under the Apache License, Version 2.0 (the
    * "License"); you may not use this file except in compliance
    * with the License. You may obtain a copy of the License at
    *
    * http://www.apache.org/licenses/LICENSE-2.0
    *
    * Unless required by applicable law or agreed to in writing, software
    * distributed under the License is distributed on an "AS IS" BASIS,
    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    * See the License for the specific language governing permissions and
    * limitations under the License.
    */
%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.commons.lang.math.NumberUtils" %>
<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.history.HistoryReader" %>
<%@ page import="org.apache.tajo.util.history.TaskHistory" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.List" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  HistoryReader reader = master.getContext().getHistoryReader();

  String queryId = request.getParameter("queryId");
  String ebId = request.getParameter("ebid");
  String startTime = request.getParameter("startTime");
  String status = request.getParameter("status");

  if(status == null || status.isEmpty() || "null".equals(status)) {
      status = "ALL";
  }

  String taskAttemptId = request.getParameter("taskAttemptId");

  List<TaskHistory> allTasks = reader.getTaskHistory(queryId, ebId, NumberUtils.toLong(startTime, 0));

  TaskHistory task = null;
  for(TaskHistory eachTask: allTasks) {
    if (eachTask.getId().equals(taskAttemptId)) {
      task = eachTask;
      break;
    }
  }

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  String backUrl = request.getHeader("referer");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/style.css"/>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Query Unit Detail</title>
</head>
<body>
<%
  if (task == null) {
%>
    <div>No Task history.</div>
    <div><a href="<%=backUrl%>">Back</a></div>
<%
    return;
  }

  String fragmentInfo = "";
  String delim = "";

  for (String eachFragment : task.getFragments()) {
      fragmentInfo += delim + eachFragment;
      delim = "<br/>";
  }

  String fetchInfo = "";
  delim = "";
  String previousKey = null;
  for (String[] e : task.getFetchs()) {
    if (previousKey == null || !previousKey.equals(e[0])) {
      fetchInfo += delim + "<b>" + e[0] + "</b>";
    }
    delim = "<br/>";
    fetchInfo += delim + e[1];

    previousKey = e[0];
  }

  String dataLocationInfos = "";
  delim = "";
  for (String eachLocation: task.getDataLocations()) {
    dataLocationInfos += delim + eachLocation;
    delim = "<br/>";
  }

  int numShuffles = task.getNumShuffles();
  String shuffleKey = "-";
  String shuffleFileName = "-";
  if(numShuffles > 0) {
    shuffleKey = task.getShuffleKey();
    shuffleFileName = task.getShuffleFileName();
  }

  String masterLabel = master.getContext().getTajoMasterService().getBindAddress().getHostName()+ ":"
          + master.getContext().getTajoMasterService().getBindAddress().getPort();
%>


<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Master: <%=masterLabel%> <%=JSPUtil.getMasterActiveLabel(master.getContext())%></h2>
  <hr/>
  <h3><a href='<%=backUrl%>'><%=ebId%></a></h3>
  <hr/>
  <table border="1" width="100%" class="border_table">
    <tr><td width="200" align="right">ID</td><td><%=task.getId()%></td></tr>
    <tr><td align="right">Progress</td><td><%=JSPUtil.percentFormat(task.getProgress())%>%</td></tr>
    <tr><td align="right">State</td><td><%=task.getState()%></td></tr>
    <tr><td align="right">Launch Time</td><td><%=task.getLaunchTime() == 0 ? "-" : df.format(task.getLaunchTime())%></td></tr>
    <tr><td align="right">Finish Time</td><td><%=task.getFinishTime() == 0 ? "-" : df.format(task.getFinishTime())%></td></tr>
    <tr><td align="right">Running Time</td><td><%=task.getLaunchTime() == 0 ? "-" : task.getRunningTime() + " ms"%></td></tr>
    <tr><td align="right">Host</td><td><%=task.getHostAndPort() == null ? "-" : task.getHostAndPort()%></td></tr>
    <tr><td align="right">Shuffles</td><td># Shuffle Outputs: <%=numShuffles%>, Shuffle Key: <%=shuffleKey%>, Shuffle file: <%=shuffleFileName%></td></tr>
    <tr><td align="right">Data Locations</td><td><%=dataLocationInfos%></td></tr>
    <tr><td align="right">Fragment</td><td><%=fragmentInfo%></td></tr>
    <tr><td align="right">Fetches</td><td><%=fetchInfo%></td></tr>
  </table>
</div>
</body>
</html>