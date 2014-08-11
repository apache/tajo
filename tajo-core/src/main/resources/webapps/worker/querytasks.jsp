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
<%@ page import="org.apache.tajo.QueryUnitAttemptId" %>
<%@ page import="org.apache.tajo.catalog.statistics.TableStats" %>
<%@ page import="org.apache.tajo.engine.planner.PlannerUtil" %>
<%@ page import="org.apache.tajo.ipc.TajoMasterProtocol" %>
<%@ page import="org.apache.tajo.master.querymaster.*" %>
<%@ page import="org.apache.tajo.util.FileUtil" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="java.text.NumberFormat" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.HashMap" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Locale" %>
<%@ page import="java.util.Map" %>

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

  List<TajoMasterProtocol.WorkerResourceProto> allWorkers = tajoWorker.getWorkerContext()
            .getQueryMasterManagerService().getQueryMaster().getAllWorker();

  Map<String, TajoMasterProtocol.WorkerResourceProto> workerMap = new HashMap<String, TajoMasterProtocol.WorkerResourceProto>();
  if(allWorkers != null) {
    for(TajoMasterProtocol.WorkerResourceProto eachWorker: allWorkers) {
      workerMap.put(eachWorker.getHost(), eachWorker);
    }
  }
  QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
          .getQueryMasterManagerService().getQueryMaster().getQueryMasterTask(queryId, true);

  if(queryMasterTask == null) {
    out.write("<script type='text/javascript'>alert('no query'); history.back(0); </script>");
    return;
  }

  Query query = queryMasterTask.getQuery();
  SubQuery subQuery = query.getSubQuery(ebid);

  if(subQuery == null) {
    out.write("<script type='text/javascript'>alert('no sub-query'); history.back(0); </script>");
    return;
  }

  if(subQuery == null) {
%>
<script type="text/javascript">
  alert("No Execution Block for" + ebid);
  document.history.back();
</script>
<%
    return;
  }

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  String url = "querytasks.jsp?queryId=" + queryId + "&ebid=" + ebid + "&status=" + status + "&sortOrder=" + nextSortOrder + "&sort=";
  QueryUnit[] queryUnits = subQuery.getQueryUnits();


  long totalInputBytes = 0;
  long totalReadBytes = 0;
  long totalReadRows = 0;
  long totalWriteBytes = 0;
  long totalWriteRows = 0;
  int numTasks = queryUnits.length;
//  int numSucceededTasks = 0;
//  int localReadTasks = subQuery.;
  int numShuffles = 0;

  float totalProgress = 0.0f;
  for(QueryUnit eachQueryUnit: queryUnits) {
    totalProgress += eachQueryUnit.getLastAttempt() != null ? eachQueryUnit.getLastAttempt().getProgress(): 0.0f;
    numShuffles = eachQueryUnit.getShuffleOutpuNum();
    if (eachQueryUnit.getLastAttempt() != null) {
      TableStats inputStats = eachQueryUnit.getLastAttempt().getInputStats();
      if (inputStats != null) {
        totalInputBytes += inputStats.getNumBytes();
        totalReadBytes += inputStats.getReadBytes();
        totalReadRows += inputStats.getNumRows();
      }
      TableStats outputStats = eachQueryUnit.getLastAttempt().getResultStats();
      if (outputStats != null) {
        totalWriteBytes += outputStats.getNumBytes();
        totalWriteRows += outputStats.getNumRows();
      }
    }
  }

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
  <pre style="white-space:pre-wrap;"><%=PlannerUtil.buildExplainString(subQuery.getBlock().getPlan())%></pre>
  <p/>
  <table border="1" width="100%" class="border_table">
    <tr><td align='right' width='180px'>Status:</td><td><%=subQuery.getState()%></td></tr>
    <tr><td align='right'>Started:</td><td><%=df.format(subQuery.getStartTime())%> ~ <%=subQuery.getFinishTime() == 0 ? "-" : df.format(subQuery.getFinishTime())%></td></tr>
    <tr><td align='right'># Tasks:</td><td><%=numTasks%> (Local Tasks: <%=subQuery.getTaskScheduler().getHostLocalAssigned()%>, Rack Local Tasks: <%=subQuery.getTaskScheduler().getRackLocalAssigned()%>)</td></tr>
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
        <option value="SCHEDULED" <%="SCHEDULED".equals(status) ? "selected" : ""%>>SCHEDULED</option>
        <option value="RUNNING" <%="RUNNING".equals(status) ? "selected" : ""%>>RUNNING</option>
        <option value="SUCCEEDED" <%="SUCCEEDED".equals(status) ? "selected" : ""%>>SUCCEEDED</option>
    </select>
    &nbsp;&nbsp;
    <input type="submit" value="Filter">
    <input type="hidden" name="queryId" value="<%=paramQueryId%>"/>
    <input type="hidden" name="ebid" value="<%=paramEbId%>"/>
    <input type="hidden" name="sort" value="<%=sort%>"/>
    <input type="hidden" name="sortOrder" value="<%=sortOrder%>"/>
  </form>
  <table border="1" width="100%" class="border_table">
    <tr><th>No</th><th><a href='<%=url%>id'>Id</a></th><th>Status</th><th>Progress</th><th><a href='<%=url%>startTime'>Started</a></th><th><a href='<%=url%>runTime'>Running Time</a></th><th><a href='<%=url%>host'>Host</a></th></tr>
    <%
      JSPUtil.sortQueryUnit(queryUnits, sort, sortOrder);
      int rowNo = 1;
      for(QueryUnit eachQueryUnit: queryUnits) {
          if(!"ALL".equals(status)) {
            if(!status.equals(eachQueryUnit.getState().toString())) {
              continue;
            }
          }
          int queryUnitSeq = eachQueryUnit.getId().getId();
          String queryUnitDetailUrl = "queryunit.jsp?queryId=" + paramQueryId + "&ebid=" + paramEbId +
                  "&queryUnitSeq=" + queryUnitSeq + "&sort=" + sort + "&sortOrder=" + sortOrder;

          String queryUnitHost = eachQueryUnit.getSucceededHost() == null ? "-" : eachQueryUnit.getSucceededHost();
          if(eachQueryUnit.getSucceededHost() != null) {
              TajoMasterProtocol.WorkerResourceProto worker = workerMap.get(eachQueryUnit.getSucceededHost());
              if(worker != null) {
                  QueryUnitAttempt lastAttempt = eachQueryUnit.getLastAttempt();
                  if(lastAttempt != null) {
                    QueryUnitAttemptId lastAttemptId = lastAttempt.getId();
                    queryUnitHost = "<a href='http://" + eachQueryUnit.getSucceededHost() + ":" + worker.getInfoPort() + "/taskdetail.jsp?queryUnitAttemptId=" + lastAttemptId + "'>" + eachQueryUnit.getSucceededHost() + "</a>";
                  }
              }
          }

    %>
    <tr>
      <td><%=rowNo%></td>
      <td><a href="<%=queryUnitDetailUrl%>"><%=eachQueryUnit.getId()%></a></td>
      <td><%=eachQueryUnit.getState()%></td>
      <td><%=JSPUtil.percentFormat(eachQueryUnit.getLastAttempt().getProgress())%>%</td>
      <td><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : df.format(eachQueryUnit.getLaunchTime())%></td>
      <td align='right'><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : eachQueryUnit.getRunningTime() + " ms"%></td>
      <td><%=queryUnitHost%></td>
    </tr>
    <%
        rowNo++;
      }
    %>
  </table>
  <%
  %>
</div>
</body>
</html>