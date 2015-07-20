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

<%@ page import="org.apache.tajo.querymaster.Query" %>
<%@ page import="org.apache.tajo.querymaster.QueryMasterTask" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.List" %>

<%
  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>tajo worker</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Worker: <%=tajoWorker.getWorkerContext().getWorkerName()%></h2>
  <hr/>
  <table border=0>
    <tr><td width='100'>MaxHeap: </td><td><%=Runtime.getRuntime().maxMemory()/1024/1024%> MB</td>
    <tr><td width='100'>TotalHeap: </td><td><%=Runtime.getRuntime().totalMemory()/1024/1024%> MB</td>
    <tr><td width='100'>FreeHeap: </td><td><%=Runtime.getRuntime().freeMemory()/1024/1024%> MB</td>
    <tr><td width='100'>Available Resource: </td><td><%= tajoWorker.getWorkerContext().getNodeResourceManager().getAvailableResource() %></td>
    <tr><td width='100'>Running Tasks: </td><td><%= tajoWorker.getWorkerContext().getTaskManager().getRunningTasks() %></td>
    <tr><td width="100">Configuration:</td><td><a href='conf.jsp'>detail...</a></td></tr>
    <tr><td width="100">Environment:</td><td><a href='env.jsp'>detail...</a></td></tr>
    <tr><td width="100">Threads:</td><td><a href='thread.jsp'>thread dump...</a></tr>
  </table>
  <hr/>

<%
  List<QueryMasterTask> queryMasterTasks = JSPUtil.sortQueryMasterTask(tajoWorker.getWorkerContext()
          .getQueryMasterManagerService().getQueryMaster().getQueryMasterTasks(), true);

%>
  <h3>Running Query</h3>
  <%
    if(queryMasterTasks.isEmpty()) {
      out.write("No running query master");
    } else {
  %>
  <table width="100%" border="1" class="border_table">
    <tr><th>QueryId</th><th>Status</th><th>StartTime</th><th>FinishTime</th><th>Progress</th><th>RunTime</th></tr>
    <%
      for(QueryMasterTask eachQueryMasterTask: queryMasterTasks) {
        Query query = eachQueryMasterTask.getQuery();
    %>
    <tr>
      <td align='center'><a href='querydetail.jsp?queryId=<%=query.getId()%>'><%=query.getId()%></a></td>
      <td align='center'><%=eachQueryMasterTask.getState()%></td>
      <td align='center'><%=df.format(query.getStartTime())%></td>
      <td align='center'><%=query.getFinishTime() == 0 ? "-" : df.format(query.getFinishTime())%></td>
      <td align='center'><%=(int)(query.getProgress()*100.0f)%>%</td>
      <td align='right'><%=JSPUtil.getElapsedTime(query.getStartTime(), query.getFinishTime())%></td>
    </tr>
    <%
        } //end of for
      } //end of if
    %>
  </table>
</div>
</body>
</html>