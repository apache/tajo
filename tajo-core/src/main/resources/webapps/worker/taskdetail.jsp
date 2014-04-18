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

<%@ page import="org.apache.tajo.QueryUnitAttemptId" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="org.apache.tajo.worker.Task" %>
<%@ page import="org.apache.tajo.worker.TaskHistory" %>
<%@ page import="org.apache.tajo.worker.TaskRunner" %>
<%@ page import="java.text.SimpleDateFormat" %>

<%
    TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

    String containerId = request.getParameter("containerId");
    String quAttemptId = request.getParameter("queryUnitAttemptId");
    QueryUnitAttemptId queryUnitAttemptId = TajoIdUtils.parseQueryUnitAttemptId(quAttemptId);
    Task task = null;
    TaskHistory taskHistory = null;
    if(containerId == null || containerId.isEmpty() || "null".equals(containerId)) {
        task = tajoWorker.getWorkerContext().getTaskRunnerManager().findTaskByQueryUnitAttemptId(queryUnitAttemptId);
        if (task != null) {
            taskHistory = task.getTaskHistory();
        } else {
            taskHistory = tajoWorker.getWorkerContext().getTaskRunnerManager().findTaskHistoryByQueryUnitAttemptId(queryUnitAttemptId);
        }
    } else {
        TaskRunner taskRunner = tajoWorker.getWorkerContext().getTaskRunnerManager().findTaskRunner(containerId);
        if(taskRunner != null) {
            task = taskRunner.getContext().getTask(queryUnitAttemptId);
            if (task != null) {
                taskHistory = task.getTaskHistory();
            } else {
                taskHistory = taskRunner.getContext().getTaskHistory(queryUnitAttemptId);
            }
        }
    }
    if(taskHistory == null) {
%>
<script type="text/javascript">
    alert("No Task Info for" + quAttemptId);
    document.history.back();
</script>
<%
        return;
    }

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
    <h2>Tajo Worker: <a href='index.jsp'><%=tajoWorker.getWorkerContext().getWorkerName()%></a></h2>
    <hr/>
    <h3>Task Detail: <%=quAttemptId%></h3>
    <table border="1" width="100%" class="border_table">
        <tr><td width="200" align="right">ID</td><td><%=quAttemptId%></td></tr>
        <tr><td align="right">State</td><td><%=taskHistory.getStatus()%></td></tr>
        <tr><td align="right">Start Time</td><td><%=taskHistory.getStartTime() == 0 ? "-" : df.format(taskHistory.getStartTime())%></td></tr>
        <tr><td align="right">Finish Time</td><td><%=taskHistory.getFinishTime() == 0 ? "-" : df.format(taskHistory.getFinishTime())%></td></tr>
        <tr><td align="right">Running Time</td><td><%=JSPUtil.getElapsedTime(taskHistory.getStartTime(), taskHistory.getFinishTime())%></td></tr>
        <tr><td align="right">Progress</td><td><%=JSPUtil.percentFormat(taskHistory.getProgress())%>%</td></tr>
        <tr><td align="right">Output Path</td><td><%=taskHistory.getOutputPath()%></td></tr>
        <tr><td align="right">Working Path</td><td><%=taskHistory.getWorkingPath()%></td></tr>
        <tr><td align="right">Input Statistics</td><td><%=TaskHistory.toInputStatsString(taskHistory.getInputStats())%></td></tr>
        <tr><td align="right">Output Statistics</td><td><%=TaskHistory.toOutputStatsString(taskHistory.getOutputStats())%></td></tr>
    </table>

<%
    if(taskHistory.hasFetcher()) {
%>
    <hr/>
    <h3>Fetch Status</h3>
    <table border="1" width="100%" class="border_table">
        <tr><th>No</th><th>StartTime</th><th>FinishTime</th><th>RunTime</th><th>Status</th><th>File Length</th><th># Messages</th><th>URI</th></tr>
<%
    int index = 1;
    for(TaskHistory.FetcherHistory eachFetcher: taskHistory.getFetchers()) {
%>
        <tr>
            <td><%=index%></td>
            <td><%=df.format(eachFetcher.getStartTime())%></td>
            <td><%=eachFetcher.getFinishTime() == 0 ? "-" : df.format(eachFetcher.getFinishTime())%></td>
            <td><%=JSPUtil.getElapsedTime(eachFetcher.getStartTime(), eachFetcher.getFinishTime())%></td>
            <td><%=eachFetcher.getStatus()%></td>
            <td align="right"><%=eachFetcher.getFileLen()%></td>
            <td align="right"><%=eachFetcher.getMessageReceiveCount()%></td>
            <td><a href="<%=eachFetcher.getUri()%>"><%=eachFetcher.getUri()%></a></td>
        </tr>
<%
        index++;
    }
%>
    </table>
<%
    }
%>
</div>
</body>
</html>