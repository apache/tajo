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

<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.text.NumberFormat" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.util.history.HistoryReader" %>
<%@ page import="org.apache.tajo.util.history.QueryHistory" %>
<%@ page import="org.apache.tajo.util.history.StageHistory" %>
<%@ page import="org.apache.tajo.master.rm.NodeStatus" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.util.history.TaskHistory" %>
<%@ page import="org.apache.tajo.util.*" %>
<%@ page import="org.apache.commons.lang.math.NumberUtils" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  HistoryReader reader = master.getContext().getHistoryReader();

  String queryId = request.getParameter("queryId");
  String startTime = request.getParameter("startTime");
  String ebId = request.getParameter("ebid");

  QueryHistory queryHistory = reader.getQueryHistory(queryId, NumberUtils.toLong(startTime, 0));

  List<StageHistory> stageHistories =
      queryHistory != null ? JSPUtil.sortStageHistories(queryHistory.getStageHistories()) : null;

  StageHistory stage = null;
  if (stageHistories != null) {
    for (StageHistory eachStage: stageHistories) {
      if (eachStage.getExecutionBlockId().equals(ebId)) {
        stage = eachStage;
        break;
      }
    }
  }

  String sort = request.getParameter("sort");
  if(sort == null) {
    sort = "id";
  }
  String sortOrder = request.getParameter("sortOrder");
  if(sortOrder == null) {
    sortOrder = "asc";
  }

  String nextSortOrder = "asc";
  if("asc".equals(sortOrder)) {
    nextSortOrder = "desc";
  }

  String status = request.getParameter("status");
  if(status == null || status.isEmpty() || "null".equals(status)) {
    status = "ALL";
  }

  Collection<NodeStatus> allWorkers = master.getContext().getResourceManager().getNodes().values();

  Map<String, NodeStatus> nodeMap = new HashMap<>();
  if(allWorkers != null) {
    for(NodeStatus eachWorker: allWorkers) {
      nodeMap.put(eachWorker.getConnectionInfo().getHostAndPeerRpcPort(), eachWorker);
    }
  }

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  long totalInputBytes = 0;
  long totalReadBytes = 0;
  long totalReadRows = 0;
  long totalWriteBytes = 0;
  long totalWriteRows = 0;

  if (stage != null) {
    totalInputBytes = stage.getTotalInputBytes();
    totalReadBytes = stage.getTotalReadBytes();
    totalReadRows = stage.getTotalReadRows();
    totalWriteBytes = stage.getTotalWriteBytes();
    totalWriteRows = stage.getTotalWriteRows();
  }

  List<TaskHistory> allTasks = reader.getTaskHistory(queryId, ebId, NumberUtils.toLong(startTime, 0));
  int numTasks = allTasks.size();
  int numShuffles = 0;
  float totalProgress = 0.0f;

  if (allTasks != null) {
    for(TaskHistory eachTask: allTasks) {
      totalProgress += eachTask.getProgress();
      numShuffles = eachTask.getNumShuffles();
    }
  }

  int currentPage = 1;
  if (request.getParameter("page") != null && !request.getParameter("page").isEmpty()) {
    currentPage = Integer.parseInt(request.getParameter("page"));
  }
  int pageSize = HistoryReader.DEFAULT_TASK_PAGE_SIZE;
  if (request.getParameter("pageSize") != null && !request.getParameter("pageSize").isEmpty()) {
    try {
      pageSize = Integer.parseInt(request.getParameter("pageSize"));
    } catch (NumberFormatException e) {
      pageSize = HistoryReader.DEFAULT_TASK_PAGE_SIZE;
    }
  }

  String url = "querytasks.jsp?queryId=" + queryId + "&ebid=" + ebId + "&startTime=" + startTime +
      "&page=" + currentPage + "&pageSize=" + pageSize +
      "&status=" + status + "&sortOrder=" + nextSortOrder + "&sort=";

  String pageUrl = "querytasks.jsp?queryId=" + queryId + "&ebid=" + ebId + "&startTime=" + startTime +
      "&status=" + status + "&sortOrder=" + nextSortOrder + "&sort=";

  NumberFormat nf = NumberFormat.getInstance(Locale.US);

  String masterLabel = master.getContext().getTajoMasterService().getBindAddress().getHostName()+ ":"
          + master.getContext().getTajoMasterService().getBindAddress().getPort();
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
  <h2>Tajo Master: <%=masterLabel%> <%=JSPUtil.getMasterActiveLabel(master.getContext())%></h2>
  <hr/>
  <h3><a href='querydetail.jsp?queryId=<%=queryId%>&startTime=<%=startTime%>'><%=ebId%></a></h3>
  <hr/>
  <p/>
  <pre style="white-space:pre-wrap;"><%=stage.getPlan()%></pre>
  <p/>
  <table border="1" width="100%" class="border_table">
    <tr><td align='right' width='180px'>Status:</td><td><%=stage.getState()%></td></tr>
    <tr><td align='right'>Started:</td><td><%=df.format(stage.getStartTime())%> ~ <%=stage.getFinishTime() == 0 ? "-" : df.format(stage.getFinishTime())%></td></tr>
    <tr><td align='right'># Tasks:</td><td><%=numTasks%> (Local Tasks: <%=stage.getHostLocalAssigned()%>, Rack Local Tasks: <%=stage.getRackLocalAssigned()%>)</td></tr>
    <tr><td align='right'>Progress:</td><td><%=JSPUtil.percentFormat((float) (totalProgress / numTasks))%>%</td></tr>
    <tr><td align='right'># Shuffles:</td><td><%=numShuffles%></td></tr>
    <tr><td align='right'>Input Bytes:</td><td><%=FileUtil.humanReadableByteCount(totalInputBytes, false) + " (" + nf.format(totalInputBytes) + " B)"%></td></tr>
    <tr><td align='right'>Actual Processed Bytes:</td><td><%=totalReadBytes == 0 ? "-" : FileUtil.humanReadableByteCount(totalReadBytes, false) + " (" + nf.format(totalReadBytes) + " B)"%></td></tr>
    <tr><td align='right'>Input Rows:</td><td><%=nf.format(totalReadRows)%></td></tr>
    <tr><td align='right'>Output Bytes:</td><td><%=FileUtil.humanReadableByteCount(totalWriteBytes, false) + " (" + nf.format(totalWriteBytes) + " B)"%></td></tr>
    <tr><td align='right'>Output Rows:</td><td><%=nf.format(totalWriteRows)%></td></tr>
  </table>
  <hr/>

  <form action='querytasks.jsp' method='GET'>
  Status:
    <select name="status" onchange="this.form.submit()">
        <option value="ALL" <%="ALL".equals(status) ? "selected" : ""%>>ALL</option>
        <option value="TA_ASSIGNED" <%="TA_ASSIGNED".equals(status) ? "selected" : ""%>>TA_ASSIGNED</option>
        <option value="TA_PENDING" <%="TA_PENDING".equals(status) ? "selected" : ""%>>TA_PENDING</option>
        <option value="TA_RUNNING" <%="TA_RUNNING".equals(status) ? "selected" : ""%>>TA_RUNNING</option>
        <option value="TA_SUCCEEDED" <%="TA_SUCCEEDED".equals(status) ? "selected" : ""%>>TA_SUCCEEDED</option>
        <option value="TA_FAILED" <%="TA_FAILED".equals(status) ? "selected" : ""%>>TA_FAILED</option>
    </select>
    &nbsp;&nbsp;
    Page Size: <input type="text" name="pageSize" value="<%=pageSize%>" size="5"/>
    &nbsp;&nbsp;
    <input type="submit" value="Filter">
    <input type="hidden" name="queryId" value="<%=queryId%>"/>
    <input type="hidden" name="ebid" value="<%=ebId%>"/>
    <input type="hidden" name="sort" value="<%=sort%>"/>
    <input type="hidden" name="sortOrder" value="<%=sortOrder%>"/>
    <input type="hidden" name="startTime" value="<%=startTime%>"/>
  </form>
<%
  List<TaskHistory> filteredTasks = new ArrayList<>();
  for(TaskHistory eachTask: allTasks) {
    if (!"ALL".equals(status)) {
      if (!status.equals(eachTask.getState())) {
        continue;
      }
    }
    filteredTasks.add(eachTask);
  }
  JSPUtil.sortTaskHistory(filteredTasks, sort, sortOrder);
  List<TaskHistory> tasks = JSPUtil.getPageNavigationList(filteredTasks, currentPage, pageSize);

  int numOfTasks = filteredTasks.size();
  int totalPage = numOfTasks % pageSize == 0 ?
      numOfTasks / pageSize : numOfTasks / pageSize + 1;
%>
  <div align="right"># Tasks: <%=numOfTasks%> / # Pages: <%=totalPage%></div>
  <table border="1" width="100%" class="border_table">
    <tr><th>No</th><th><a href='<%=url%>id'>Id</a></th><th>Status</th><th>Progress</th><th><a href='<%=url%>startTime'>Started</a></th><th><a href='<%=url%>runTime'>Running Time</a></th><th>Retry</th><th><a href='<%=url%>host'>Host</a></th></tr>
<%
  int rowNo = (currentPage - 1) * pageSize + 1;
  for (TaskHistory eachTask: tasks) {
    String taskDetailUrl = "";
    if (eachTask.getId() != null) {
      taskDetailUrl = "task.jsp?queryId=" + queryId + "&ebid=" + ebId + "&startTime=" + startTime +
          "&taskAttemptId=" + eachTask.getId() + "&sort=" + sort + "&sortOrder=" + sortOrder;
    }
    String taskHost = eachTask.getHostAndPort() == null ? "-" : eachTask.getHostAndPort();
    if (eachTask.getHostAndPort() != null) {
      NodeStatus nodeStatus = nodeMap.get(eachTask.getHostAndPort());
      if (nodeStatus != null) {
        String[] hostTokens = eachTask.getHostAndPort().split(":");
        taskHost = "<a href='http://" + hostTokens[0] + ":" + nodeStatus.getConnectionInfo().getHttpInfoPort() +
            "/taskhistory.jsp?taskAttemptId=" + eachTask.getId() + "&startTime=" + eachTask.getLaunchTime() +
            "'>" + eachTask.getHostAndPort() + "</a>";
      }
    }

%>
    <tr>
      <td align='center'><%=rowNo%></td>
      <td><a href="<%=taskDetailUrl%>"><%=eachTask.getId()%></a></td>
      <td align='center'><%=eachTask.getState()%></td>
      <td align='center'><%=JSPUtil.percentFormat(eachTask.getProgress())%>%</td>
      <td align='center'><%=eachTask.getLaunchTime() == 0 ? "-" : df.format(eachTask.getLaunchTime())%></td>
      <td align='right'><%=eachTask.getLaunchTime() == 0 ? "-" : eachTask.getRunningTime() + " ms"%></td>
      <td align='center'><%=eachTask.getRetryCount()%></td>
      <td><%=taskHost%></td>
    </tr>
    <%
        rowNo++;
      }
    %>
  </table>
  <div align="center">
    <%=JSPUtil.getPageNavigation(currentPage, totalPage, pageUrl + "&pageSize=" + pageSize)%>
  </div>
  <p/>
</div>
</body>
</html>