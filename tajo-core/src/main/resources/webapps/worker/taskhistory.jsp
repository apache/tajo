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

<%@ page import="org.apache.tajo.ipc.TajoWorkerProtocol" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.*" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.List" %>
<%@ page import="org.apache.tajo.util.history.HistoryReader" %>

<%
  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  HistoryReader reader = new HistoryReader(tajoWorker.getWorkerContext().getWorkerName(), tajoWorker.getWorkerContext().getConf());

  TaskHistory taskHistory = reader.getTaskHistory(request.getParameter("taskAttemptId"),
      Long.parseLong(request.getParameter("startTime")));

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  String referer = request.getHeader("referer");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/style.css"/>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>tajo worker</title>
</head>
<body>
<%@ include file="header.jsp"%>
<%
  if (taskHistory == null) {
%>
<div class='contents'>
  <div>No Task history</div>
  <div><a href="<%=referer%>">Back</a></div>
</div>
</body>
</html>
<%
    return;
  }   //end of if [taskHistory == null]
  %>
<div class='contents'>
  <h2>Tajo Worker: <a href='index.jsp'><%=tajoWorker.getWorkerContext().getWorkerName()%></a></h2>
  <hr/>
  <h3>Task Detail: <%=request.getParameter("taskAttemptId")%></h3>
  <table border="1" width="100%" class="border_table">
      <tr><td width="200" align="right">ID</td><td><%=request.getParameter("taskAttemptId")%></td></tr>
      <tr><td align="right">State</td><td><%=taskHistory.getState()%></td></tr>
      <tr><td align="right">Start Time</td><td><%=taskHistory.getStartTime() == 0 ? "-" : df.format(taskHistory.getStartTime())%></td></tr>
      <tr><td align="right">Finish Time</td><td><%=taskHistory.getFinishTime() == 0 ? "-" : df.format(taskHistory.getFinishTime())%></td></tr>
      <tr><td align="right">Running Time</td><td><%=JSPUtil.getElapsedTime(taskHistory.getStartTime(), taskHistory.getFinishTime())%></td></tr>
      <tr><td align="right">Progress</td><td><%=JSPUtil.percentFormat(taskHistory.getProgress())%>%</td></tr>
      <tr><td align="right">Output Path</td><td><%=taskHistory.getOutputPath()%></td></tr>
      <tr><td align="right">Working Path</td><td><%=taskHistory.getWorkingPath()%></td></tr>
      <tr><td align="right">Input Statistics</td><td><%=JSPUtil.tableStatToString(taskHistory.getInputStats())%></td></tr>
      <tr><td align="right">Output Statistics</td><td><%=JSPUtil.tableStatToString(taskHistory.getOutputStats())%></td></tr>
  </table>
  <hr/>
<%
  if (taskHistory.hasFetcherHistories()) {
%>
  <h3>Fetch Status &nbsp;
      <span><%= taskHistory.getFinishedFetchCount() + "/" + taskHistory.getTotalFetchCount() %> (Finished/Total)</span>
  </h3>
<%
    int index = 1;
    int pageSize = 1000; //TODO pagination

    List<TajoWorkerProtocol.FetcherHistoryProto> fetcherHistories = taskHistory.getFetcherHistories();
    if (fetcherHistories.size() > 0) {
%>
  <table border="1" width="100%" class="border_table">
  <tr><th>No</th><th>StartTime</th><th>FinishTime</th><th>RunTime</th><th>Status</th><th>File Length</th><th># Messages</th></tr>
<%
      for (TajoWorkerProtocol.FetcherHistoryProto eachFetcher : fetcherHistories) {
%>
    <tr>
      <td><%=index%></td>
      <td><%=df.format(eachFetcher.getStartTime())%></td>
      <td><%=eachFetcher.getFinishTime() == 0 ? "-" : df.format(eachFetcher.getFinishTime())%></td>
      <td><%=JSPUtil.getElapsedTime(eachFetcher.getStartTime(), eachFetcher.getFinishTime())%></td>
      <td><%=eachFetcher.getState()%></td>
      <td align="right"><%=eachFetcher.getFileLength()%></td>
      <td align="right"><%=eachFetcher.getMessageReceivedCount()%></td>
    </tr>
<%
        index++;
        if (pageSize < index) {
%>
    <tr>
      <td colspan="8">has more ...</td>
    </tr>
<%
            break;
        }
      }   // end of for loop
    }   // end of if [fetcherHistories.size() > 0]
  }
%>
  </table>
</div>
</body>
</html>