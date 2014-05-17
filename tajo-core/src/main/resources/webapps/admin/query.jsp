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

<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryInProgress" %>
<%@ page import="org.apache.tajo.master.rm.Worker" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.StringUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.*" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  List<QueryInProgress> runningQueries =
          new ArrayList<QueryInProgress>(master.getContext().getQueryJobManager().getSubmittedQueries());

  runningQueries.addAll(master.getContext().getQueryJobManager().getRunningQueries());
          JSPUtil.sortQueryInProgress(runningQueries, true);

  List<QueryInProgress> finishedQueries =
          JSPUtil.sortQueryInProgress(master.getContext().getQueryJobManager().getFinishedQueries(), true);

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  Map<String, Worker> workers = master.getContext().getResourceManager().getWorkers();
  Map<String, Integer> portMap = new HashMap<String, Integer>();

  Collection<String> queryMasters = master.getContext().getResourceManager().getQueryMasters();
  if (queryMasters == null || queryMasters.isEmpty()) {
    queryMasters = master.getContext().getResourceManager().getWorkers().keySet();
  }
  for(String eachQueryMasterKey: queryMasters) {
    Worker queryMaster = workers.get(eachQueryMasterKey);
    if(queryMaster != null) {
      portMap.put(queryMaster.getHostName(), queryMaster.getHttpPort());
    }
  }
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Tajo</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Master: <%=master.getMasterName()%></h2>
  <hr/>
  <h3>Running Queries</h3>
<%
  if(runningQueries.isEmpty()) {
    out.write("No running queries");
  } else {
%>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>QueryId</th><th>Query Master</th><th>Started</th><th>Progress</th><th>Time</th><th>Status</th></th><th>sql</th></tr>
    <%
      for(QueryInProgress eachQuery: runningQueries) {
        long time = System.currentTimeMillis() - eachQuery.getQueryInfo().getStartTime();
        String detailView = "http://" + eachQuery.getQueryInfo().getQueryMasterHost() + ":" + portMap.get(eachQuery.getQueryInfo().getQueryMasterHost()) +
                "/querydetail.jsp?queryId=" + eachQuery.getQueryId();
    %>
    <tr>
      <td><a href='<%=detailView%>'><%=eachQuery.getQueryId()%></a></td>
      <td><%=eachQuery.getQueryInfo().getQueryMasterHost()%></td>
      <td><%=df.format(eachQuery.getQueryInfo().getStartTime())%></td>
      <td><%=(int)(eachQuery.getQueryInfo().getProgress() * 100.0f)%>%</td>
      <td><%=StringUtils.formatTime(time)%></td>
      <td><%=eachQuery.getQueryInfo().getQueryState()%></td>
      <td><%=eachQuery.getQueryInfo().getSql()%></td>
    </tr>
    <%
      }
    %>
  </table>
<%
  }
%>
  <p/>
  <hr/>
  <h3>Finished Queries</h3>
  <%
    if(finishedQueries.isEmpty()) {
      out.write("No finished queries");
    } else {
  %>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>QueryId</th><th>Query Master</th><th>Started</th><th>Finished</th><th>Time</th><th>Status</th><th>sql</th></tr>
    <%
      for(QueryInProgress eachQuery: finishedQueries) {
        long runTime = eachQuery.getQueryInfo().getFinishTime() > 0 ?
                eachQuery.getQueryInfo().getFinishTime() - eachQuery.getQueryInfo().getStartTime() : -1;
        String detailView = "http://" + eachQuery.getQueryInfo().getQueryMasterHost() + ":" + portMap.get(eachQuery.getQueryInfo().getQueryMasterHost())  +
                "/querydetail.jsp?queryId=" + eachQuery.getQueryId();
    %>
    <tr>
      <td><a href='<%=detailView%>'><%=eachQuery.getQueryId()%></a></td>
      <td><%=eachQuery.getQueryInfo().getQueryMasterHost()%></td>
      <td><%=df.format(eachQuery.getQueryInfo().getStartTime())%></td>
      <td><%=eachQuery.getQueryInfo().getFinishTime() > 0 ? df.format(eachQuery.getQueryInfo().getFinishTime()) : "-"%></td>
      <td><%=runTime == -1 ? "-" : StringUtils.formatTime(runTime) %></td>
      <td><%=eachQuery.getQueryInfo().getQueryState()%></td>
      <td><%=eachQuery.getQueryInfo().getSql()%></td>
    </tr>
    <%
      }
    %>
  </table>
<%
  }
%>
</div>
</body>
</html>
