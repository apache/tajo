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

<%@ page import="org.apache.tajo.ExecutionBlockId" %>
<%@ page import="org.apache.tajo.QueryId" %>
<%@ page import="org.apache.tajo.TaskAttemptId" %>
<%@ page import="org.apache.tajo.catalog.statistics.TableStats" %>
<%@ page import="org.apache.tajo.plan.util.PlannerUtil" %>
<%@ page import="org.apache.tajo.querymaster.*" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="java.text.NumberFormat" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="org.apache.tajo.util.history.HistoryReader" %>
<%@ page import="org.apache.tajo.util.*" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.TajoProtos" %>
<%@ page import="org.apache.tajo.master.cluster.WorkerConnectionInfo" %>

<%
  String paramQueryId = request.getParameter("queryId");
  String paramEbId = request.getParameter("ebid");

  QueryId queryId = TajoIdUtils.parseQueryId(paramQueryId);
  ExecutionBlockId ebid = TajoIdUtils.createExecutionBlockId(paramEbId);
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
  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  List<TajoProtos.WorkerConnectionInfoProto> allWorkers = tajoWorker.getWorkerContext()
            .getQueryMasterManagerService().getQueryMaster().getAllWorker();

  Map<Integer, TajoProtos.WorkerConnectionInfoProto> workerMap = new HashMap<>();
  if(allWorkers != null) {
    for(TajoProtos.WorkerConnectionInfoProto eachWorker: allWorkers) {
      workerMap.put(eachWorker.getId(), eachWorker);
    }
  }
  QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
          .getQueryMasterManagerService().getQueryMaster().getQueryMasterTask(queryId);

  if(queryMasterTask == null) {
    String tajoMasterHttp = request.getScheme() + "://" + JSPUtil.getTajoMasterHttpAddr(tajoWorker.getConfig());
    response.sendRedirect(tajoMasterHttp + request.getRequestURI() + "?" + request.getQueryString());
    return;
  }

  Query query = queryMasterTask.getQuery();
  Stage stage = query.getStage(ebid);

  if(stage == null) {
    out.write("<script type='text/javascript'>alert('no sub-query'); history.back(0); </script>");
    return;
  }

  if(stage == null) {
%>
<script type="text/javascript">
  alert("No Execution Block for" + ebid);
  document.history.back();
</script>
<%
    return;
  }

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  Task[] allTasks = stage.getTasks();

  long totalInputBytes = 0;
  long totalReadBytes = 0;
  long totalReadRows = 0;
  long totalWriteBytes = 0;
  long totalWriteRows = 0;
  int numTasks = allTasks.length;
  int numShuffles = 0;

  float totalProgress = 0.0f;
  for(Task eachTask : allTasks) {

    numShuffles = eachTask.getShuffleOutpuNum();
    TaskAttempt lastAttempt = eachTask.getLastAttempt();
    if (lastAttempt != null) {
      totalProgress +=  lastAttempt.getProgress();
      TableStats inputStats = lastAttempt.getInputStats();
      if (inputStats != null) {
        totalInputBytes += inputStats.getNumBytes();
        totalReadBytes += inputStats.getReadBytes();
        totalReadRows += inputStats.getNumRows();
      }
      TableStats outputStats = lastAttempt.getResultStats();
      if (outputStats != null) {
        totalWriteBytes += outputStats.getNumBytes();
        totalWriteRows += outputStats.getNumRows();
      }
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

  String url = "querytasks.jsp?queryId=" + queryId + "&ebid=" + ebid +
      "&page=" + currentPage + "&pageSize=" + pageSize +
      "&status=" + status + "&sortOrder=" + nextSortOrder + "&sort=";

  String pageUrl = "querytasks.jsp?queryId=" + paramQueryId + "&ebid=" + paramEbId +
      "&status=" + status + "&sortOrder=" + nextSortOrder + "&sort=";

  NumberFormat nf = NumberFormat.getInstance(Locale.US);
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
  <h3><a href='querydetail.jsp?queryId=<%=paramQueryId%>'><%=ebid.toString()%></a></h3>
  <hr/>
  <p/>
  <pre style="white-space:pre-wrap;"><%=PlannerUtil.buildExplainString(stage.getBlock().getPlan())%></pre>
  <p/>
  <table border="1" width="100%" class="border_table">
    <tr><td align='right' width='180px'>Status:</td><td><%=stage.getState()%></td></tr>
    <tr><td align='right'>Started:</td><td><%=df.format(stage.getStartTime())%> ~ <%=stage.getFinishTime() == 0 ? "-" : df.format(stage.getFinishTime())%></td></tr>
    <tr><td align='right'># Tasks:</td><td><%=numTasks%> (Local Tasks: <%=stage.getTaskScheduler().getHostLocalAssigned()%>,
      Rack Local Tasks: <%=stage.getTaskScheduler().getRackLocalAssigned()%>,
      Canceled Attempt: <%=stage.getTaskScheduler().getCancellation() %>)</td></tr>
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
    <input type="hidden" name="queryId" value="<%=paramQueryId%>"/>
    <input type="hidden" name="ebid" value="<%=paramEbId%>"/>
    <input type="hidden" name="sort" value="<%=sort%>"/>
    <input type="hidden" name="sortOrder" value="<%=sortOrder%>"/>
  </form>
<%
  List<Task> filteredTask = new ArrayList<>();
  for(Task eachTask : allTasks) {
    if (!"ALL".equals(status)) {
      if (!status.equals(eachTask.getLastAttemptStatus().toString())) {
        continue;
      }
    }
    filteredTask.add(eachTask);
  }
  JSPUtil.sortTasks(filteredTask, sort, sortOrder);
  List<Task> tasks = JSPUtil.getPageNavigationList(filteredTask, currentPage, pageSize);

  int numOfTasks = filteredTask.size();
  int totalPage = numOfTasks % pageSize == 0 ?
      numOfTasks / pageSize : numOfTasks / pageSize + 1;

  int rowNo = (currentPage - 1) * pageSize + 1;
%>
  <div align="right"># Tasks: <%=numOfTasks%> / # Pages: <%=totalPage%></div>
  <table border="1" width="100%" class="border_table">
    <tr><th>No</th><th><a href='<%=url%>id'>Id</a></th><th>Status</th><th>Progress</th><th><a href='<%=url%>startTime'>Started</a></th><th><a href='<%=url%>runTime'>Running Time</a></th><th>Retry</th><th><a href='<%=url%>host'>Host</a></th></tr>
<%
  for(Task eachTask : tasks) {
    int taskSeq = eachTask.getId().getId();
    String taskDetailUrl = "task.jsp?queryId=" + paramQueryId + "&ebid=" + paramEbId +
            "&page=" + currentPage + "&pageSize=" + pageSize +
            "&taskSeq=" + taskSeq + "&sort=" + sort + "&sortOrder=" + sortOrder;

    TaskAttempt lastAttempt = eachTask.getLastAttempt();
    String taskHost = "-";
    float progress = 0.0f;
    if(lastAttempt != null && lastAttempt.getWorkerConnectionInfo() != null) {
      WorkerConnectionInfo conn = lastAttempt.getWorkerConnectionInfo();
      TaskAttemptId lastAttemptId = lastAttempt.getId();
      taskHost = "<a href='http://" + conn.getHost() + ":" + conn.getHttpInfoPort() + tajoMasterInfoAddressContextPath + "/taskdetail.jsp?taskAttemptId=" + lastAttemptId + "'>" + conn.getHost() + "</a>";
      progress = eachTask.getLastAttempt().getProgress();
    }
%>
    <tr>
      <td align='center'><%=rowNo%></td>
      <td><a href="<%=taskDetailUrl%>"><%=eachTask.getId()%></a></td>
      <td align='center'><%=eachTask.getLastAttemptStatus()%></td>
      <td align='center'><%=JSPUtil.percentFormat(progress)%>%</td>
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