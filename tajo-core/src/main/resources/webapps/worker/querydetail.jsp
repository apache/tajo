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

<%@ page import="org.apache.tajo.QueryId" %>
<%@ page import="org.apache.tajo.querymaster.Query" %>
<%@ page import="org.apache.tajo.querymaster.QueryMasterTask" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Map" %>
<%@ page import="org.apache.tajo.SessionVars" %>
<%@ page import="org.apache.tajo.util.history.QueryHistory" %>
<%@ page import="org.apache.tajo.util.history.StageHistory" %>
<%@ page import="org.apache.tajo.util.history.HistoryReader" %>

<%
  QueryId queryId = TajoIdUtils.parseQueryId(request.getParameter("queryId"));

  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
          .getQueryMasterManagerService().getQueryMaster().getQueryMasterTask(queryId, true);

  boolean runningQuery = queryMasterTask != null;

  QueryHistory queryHistory = null;

  Query query = null;
  if (queryMasterTask != null) {
    query = queryMasterTask.getQuery();
    if (query != null) {
      queryHistory = query.getQueryHistory();
    }
  } else {
    HistoryReader reader = tajoWorker.getWorkerContext().getHistoryReader();
    queryHistory = reader.getQueryHistory(queryId.toString());
  }

  if (!runningQuery && queryHistory == null) {
    out.write("<script type='text/javascript'>alert('no query history'); history.back(0); </script>");
    return;
  }

  List<StageHistory> stageHistories =
      queryHistory != null ? JSPUtil.sortStageHistories(queryHistory.getStageHistories()) : null;

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/style.css"/>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Query Detail Info</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Worker: <a href='index.jsp'><%=tajoWorker.getWorkerContext().getWorkerName()%></a></h2>
  <hr/>
<%
if (runningQuery && query == null) {
  out.write("Query Status: " + queryMasterTask.getState());
  String errorMessage = queryMasterTask.getErrorMessage();
  if (errorMessage != null && !errorMessage.isEmpty()) {
    out.write("<p/>Message:<p/><pre>" + errorMessage + "</pre>");
  }
} else if (stageHistories == null) {
  out.write("<p/>Message:<p/><pre>No Stages</pre>");
} else {
%>
  <h3><%=queryId.toString()%> <a href='queryplan.jsp?queryId=<%=queryId%>'>[Query Plan]</a></h3>
  <table width="100%" border="1" class="border_table">
    <tr><th>ID</th><th>State</th><th>Started</th><th>Finished</th><th>Running time</th><th>Progress</th><th>Tasks</th></tr>
<%
for(StageHistory eachStage: stageHistories) {
    eachStage.getSucceededObjectCount();
    String detailLink = "querytasks.jsp?queryId=" + queryId + "&ebid=" + eachStage.getExecutionBlockId();
%>
  <tr>
    <td><a href='<%=detailLink%>'><%=eachStage.getExecutionBlockId()%></a></td>
    <td><%=eachStage.getState()%></td>
    <td><%=df.format(eachStage.getStartTime())%></td>
    <td><%=eachStage.getFinishTime() == 0 ? "-" : df.format(eachStage.getFinishTime())%></td>
    <td><%=JSPUtil.getElapsedTime(eachStage.getStartTime(), eachStage.getFinishTime())%></td>
    <td align='center'><%=JSPUtil.percentFormat(eachStage.getProgress())%>%</td>
    <td align='center'><a href='<%=detailLink%>&status=SUCCEEDED'><%=eachStage.getSucceededObjectCount()%></a>/<a href='<%=detailLink%>&status=ALL'><%=eachStage.getTotalScheduledObjectsCount()%></a></td>
  </tr>
  <%
}  //end of for
  %>
  </table>
  <p/>
  <h3>Applied Session Variables</h3>
  <table width="100%" border="1" class="border_table">
  <%for(Map.Entry<String,String> entry: query.getPlan().getContext().getAllKeyValus().entrySet()) {
      if (SessionVars.exists(entry.getKey()) && SessionVars.isPublic(SessionVars.get(entry.getKey()))) {
          %> <tr><td width="200"><%=entry.getKey()%></td><td><%=entry.getValue()%></td> <%
      }
  } %>
  </table>
  <hr/>
  <h3>Logical Plan</h3>
  <pre style="white-space:pre-wrap;"><%=query.getPlan().getLogicalPlan().toString()%></pre>
  <hr/>
  <h3>Distributed Query Plan</h3>
  <pre style="white-space:pre-wrap;"><%=query.getPlan().toString()%></pre>
  <hr/>
<%
}   //end of else [if (query == null)]
%>
</div>
</body>
</html>