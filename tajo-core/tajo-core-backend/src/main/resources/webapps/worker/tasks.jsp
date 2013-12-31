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

<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.*" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="org.apache.tajo.QueryUnitAttemptId" %>

<%
    String containerId = request.getParameter("containerId");
    TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

    TaskRunner taskRunner = tajoWorker.getWorkerContext().getTaskRunnerManager().findTaskRunner(containerId);
    if(taskRunner == null) {
%>
<script type="text/javascript">
    alert("No Task Container for" + containerId);
    document.history.back();
</script>
<%
        return;
    }

    TaskRunner.TaskRunnerContext taskRunnerContext = taskRunner.getContext();
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
    <h3>Tasks</h3>
    <table width="100%" border="1" class="border_table">
        <tr><th>Id</th><th>StartTime</th><th>FinishTime</th><th>RunTime</th><th>Status</th></tr>
<%
    for(Map.Entry<QueryUnitAttemptId, Task> entry: taskRunnerContext.getTasks().entrySet()) {
        QueryUnitAttemptId queryUnitId = entry.getKey();
        TaskHistory eachTask = entry.getValue().getTaskHistory();
%>
        <tr>
            <td><a href="taskdetail.jsp?containerId=<%=containerId%>&queryUnitAttemptId=<%=queryUnitId%>"><%=queryUnitId%></a></td>
            <td><%=df.format(eachTask.getStartTime())%></td>
            <td><%=eachTask.getFinishTime() == 0 ? "-" : df.format(eachTask.getFinishTime())%></td>
            <td><%=JSPUtil.getElapsedTime(eachTask.getStartTime(), eachTask.getFinishTime())%></td>
            <td><%=eachTask.getStatus()%></td>
        </tr>
<%
    }

    for(Map.Entry<QueryUnitAttemptId, TaskHistory> entry: taskRunnerContext.getTaskHistories().entrySet()) {
        QueryUnitAttemptId queryUnitId = entry.getKey();
        TaskHistory eachTask = entry.getValue();
%>
        <tr>
            <td><a href="taskdetail.jsp?containerId=<%=containerId%>&queryUnitAttemptId=<%=queryUnitId%>"><%=queryUnitId%></a></td>
            <td><%=df.format(eachTask.getStartTime())%></td>
            <td><%=eachTask.getFinishTime() == 0 ? "-" : df.format(eachTask.getFinishTime())%></td>
            <td><%=JSPUtil.getElapsedTime(eachTask.getStartTime(), eachTask.getFinishTime())%></td>
            <td><%=eachTask.getStatus()%></td>
        </tr>
<%
    }
%>
    </table>
</div>
</body>
</html>