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

<%@ page import="org.apache.tajo.util.FileUtil" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.text.NumberFormat" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.util.history.HistoryReader" %>
<%@ page import="org.apache.tajo.util.history.QueryHistory" %>
<%@ page import="org.apache.tajo.util.history.SubQueryHistory" %>
<%@ page import="org.apache.tajo.master.rm.Worker" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.util.history.QueryUnitHistory" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  HistoryReader reader = master.getContext().getHistoryReader();

  String queryId = request.getParameter("queryId");
  String startTime = request.getParameter("startTime");
  String ebId = request.getParameter("ebid");

  QueryHistory queryHistory = reader.getQueryHistory(queryId);

  List<SubQueryHistory> subQueryHistories =
      queryHistory != null ? JSPUtil.sortSubQueryHistory(queryHistory.getSubQueryHistories()) : null;

  SubQueryHistory subQuery = null;
  if (subQueryHistories != null) {
    for (SubQueryHistory eachSubQuery: subQueryHistories) {
      if (eachSubQuery.getExecutionBlockId().equals(ebId)) {
        subQuery = eachSubQuery;
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

  Collection<Worker> allWorkers = master.getContext().getResourceManager().getWorkers().values();

  Map<String, Worker> workerMap = new HashMap<String, Worker>();
  if(allWorkers != null) {
    for(Worker eachWorker: allWorkers) {
      workerMap.put(eachWorker.getConnectionInfo().getHostAndPeerRpcPort(), eachWorker);
    }
  }

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  long totalInputBytes = 0;
  long totalReadBytes = 0;
  long totalReadRows = 0;
  long totalWriteBytes = 0;
  long totalWriteRows = 0;

  if (subQuery != null) {
    totalInputBytes = subQuery.getTotalInputBytes();
    totalReadBytes = subQuery.getTotalReadBytes();
    totalReadRows = subQuery.getTotalReadRows();
    totalWriteBytes = subQuery.getTotalWriteBytes();
    totalWriteRows = subQuery.getTotalWriteRows();
  }

  List<QueryUnitHistory> allQueryUnits = reader.getQueryUnitHistory(queryId, ebId);
  int numTasks = allQueryUnits.size();
  int numShuffles = 0;
  float totalProgress = 0.0f;

  if (allQueryUnits != null) {
    for(QueryUnitHistory eachQueryUnit: allQueryUnits) {
      totalProgress += eachQueryUnit.getProgress();
      numShuffles = eachQueryUnit.getNumShuffles();
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
  <h3><a href='querydetail.jsp?queryId=<%=queryId%>&startTime=<%=startTime%>'><%=ebId.toString()%></a></h3>
  <hr/>
  <p/>
  <pre style="white-space:pre-wrap;"><%=subQuery.getPlan()%></pre>
  <p/>
  <table border="1" width="100%" class="border_table">
    <tr><td align='right' width='180px'>Status:</td><td><%=subQuery.getState()%></td></tr>
    <tr><td align='right'>Started:</td><td><%=df.format(subQuery.getStartTime())%> ~ <%=subQuery.getFinishTime() == 0 ? "-" : df.format(subQuery.getFinishTime())%></td></tr>
    <tr><td align='right'># Tasks:</td><td><%=numTasks%> (Local Tasks: <%=subQuery.getHostLocalAssigned()%>, Rack Local Tasks: <%=subQuery.getRackLocalAssigned()%>)</td></tr>
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
  List<QueryUnitHistory> filteredQueryUnit = new ArrayList<QueryUnitHistory>();
  for(QueryUnitHistory eachQueryUnit: allQueryUnits) {
    if (!"ALL".equals(status)) {
      if (!status.equals(eachQueryUnit.getState().toString())) {
        continue;
      }
    }
    filteredQueryUnit.add(eachQueryUnit);
  }
  JSPUtil.sortQueryUnitHistory(filteredQueryUnit, sort, sortOrder);
  List<QueryUnitHistory> queryUnits = JSPUtil.getPageNavigationList(filteredQueryUnit, currentPage, pageSize);

  int numOfQueryUnits = filteredQueryUnit.size();
  int totalPage = numOfQueryUnits % pageSize == 0 ?
      numOfQueryUnits / pageSize : numOfQueryUnits / pageSize + 1;
%>
  <div align="right"># Tasks: <%=numOfQueryUnits%> / # Pages: <%=totalPage%></div>
  <table border="1" width="100%" class="border_table">
    <tr><th>No</th><th><a href='<%=url%>id'>Id</a></th><th>Status</th><th>Progress</th><th><a href='<%=url%>startTime'>Started</a></th><th><a href='<%=url%>runTime'>Running Time</a></th><th><a href='<%=url%>host'>Host</a></th></tr>
<%
  int rowNo = (currentPage - 1) * pageSize + 1;
  for (QueryUnitHistory eachQueryUnit: queryUnits) {
    String queryUnitDetailUrl = "";
    if (eachQueryUnit.getId() != null) {
      queryUnitDetailUrl = "queryunit.jsp?queryId=" + queryId + "&ebid=" + ebId + "&startTime=" + startTime +
          "&queryUnitAttemptId=" + eachQueryUnit.getId() + "&sort=" + sort + "&sortOrder=" + sortOrder;
    }
    String queryUnitHost = eachQueryUnit.getHostAndPort() == null ? "-" : eachQueryUnit.getHostAndPort();
    if (eachQueryUnit.getHostAndPort() != null) {
      Worker worker = workerMap.get(eachQueryUnit.getHostAndPort());
      if (worker != null) {
        String[] hostTokens = eachQueryUnit.getHostAndPort().split(":");
        queryUnitHost = "<a href='http://" + hostTokens[0] + ":" + worker.getConnectionInfo().getHttpInfoPort() +
            "/taskhistory.jsp?queryUnitAttemptId=" + eachQueryUnit.getId() + "&startTime=" + eachQueryUnit.getLaunchTime() +
            "'>" + eachQueryUnit.getHostAndPort() + "</a>";
      }
    }

%>
    <tr>
      <td><%=rowNo%></td>
      <td><a href="<%=queryUnitDetailUrl%>"><%=eachQueryUnit.getId()%></a></td>
      <td><%=eachQueryUnit.getState()%></td>
      <td><%=JSPUtil.percentFormat(eachQueryUnit.getProgress())%>%</td>
      <td><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : df.format(eachQueryUnit.getLaunchTime())%></td>
      <td align='right'><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : eachQueryUnit.getRunningTime() + " ms"%></td>
      <td><%=queryUnitHost%></td>
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