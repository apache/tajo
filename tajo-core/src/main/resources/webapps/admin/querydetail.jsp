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

<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.List" %>
<%@ page import="org.apache.tajo.util.history.QueryHistory" %>
<%@ page import="org.apache.tajo.util.history.SubQueryHistory" %>
<%@ page import="org.apache.tajo.util.history.HistoryReader" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  HistoryReader reader = master.getContext().getHistoryReader();

  String queryId = request.getParameter("queryId");
  String startTime = request.getParameter("startTime");
  QueryHistory queryHistory = reader.getQueryHistory(queryId, Long.parseLong(startTime));

  List<SubQueryHistory> subQueryHistories =
      queryHistory != null ? JSPUtil.sortSubQueryHistory(queryHistory.getSubQueryHistories()) : null;

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
  <h2>Tajo Master: <%=master.getMasterName()%> <%=JSPUtil.getMasterActiveLabel(master.getContext())%></h2>
  <hr/>
  <h3><%=queryId%></h3>
<%
if (queryHistory == null) {
%>
  <div>No Query history data.</div>
<%
} else {
  if (subQueryHistories == null) {
%>
    <div>No SubQuery history data.</div>
<%
  } else {
%>
  <table width="100%" border="1" class="border_table">
    <tr><th>ID</th><th>State</th><th>Started</th><th>Finished</th><th>Running time</th><th>Progress</th><th>Succeeded/Total</th><th>Failed/Killed</th></tr>
<%
    for(SubQueryHistory eachSubQuery: subQueryHistories) {
        String detailLink = "querytasks.jsp?queryId=" + queryId + "&ebid=" + eachSubQuery.getExecutionBlockId() + "&startTime=" + startTime;
%>
  <tr>
    <td><a href='<%=detailLink%>'><%=eachSubQuery.getExecutionBlockId()%></a></td>
    <td><%=eachSubQuery.getState()%></td>
    <td><%=df.format(eachSubQuery.getStartTime())%></td>
    <td><%=eachSubQuery.getFinishTime() == 0 ? "-" : df.format(eachSubQuery.getFinishTime())%></td>
    <td><%=JSPUtil.getElapsedTime(eachSubQuery.getStartTime(), eachSubQuery.getFinishTime())%></td>
    <td align='center'><%=JSPUtil.percentFormat(eachSubQuery.getProgress())%>%</td>
    <td align='center'><%=eachSubQuery.getSucceededObjectCount()%> / <%=eachSubQuery.getTotalScheduledObjectsCount()%></td>
    <td align='center'><%=eachSubQuery.getFailedObjectCount()%> / <%=eachSubQuery.getKilledObjectCount()%></td>
  </tr>
  <%
    }  //end of for
  %>
  </table>
<%
  }   //end of else [if (subQueryHistories == null)]
%>
  <p/>
  <h3>Applied Session Variables</h3>
  <table width="100%" border="1" class="border_table">
<%
  for(String[] sessionVariable: queryHistory.getSessionVariables()) {
%>
    <tr><td width="200"><%=sessionVariable[0]%></td><td><%=sessionVariable[1]%></td>
<%
  }
%>
  </table>
  <hr/>
  <h3>Logical Plan</h3>
  <pre style="white-space:pre-wrap;"><%=queryHistory.getLogicalPlan()%></pre>
  <hr/>
  <h3>Distributed Query Plan</h3>
  <pre style="white-space:pre-wrap;"><%=queryHistory.getDistributedPlan()%></pre>
  <hr/>
<%
}   //end of else [if (query == null)]
%>
</div>
</body>
</html>