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

<%@ page import="org.apache.hadoop.fs.FileSystem" %>
<%@ page import="org.apache.tajo.conf.TajoConf" %>
<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryInProgress" %>
<%@ page import="org.apache.tajo.master.rm.WorkerResource" %>
<%@ page import="org.apache.tajo.master.rm.WorkerStatus" %>
<%@ page import="org.apache.tajo.util.NetUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.util.Collection" %>
<%@ page import="java.util.Date" %>
<%@ page import="java.util.Map" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  Map<String, WorkerResource> workers = master.getContext().getResourceManager().getWorkers();

  int numLiveWorkers = 0;
  int numDeadWorkers = 0;
  int numDecommissionWorkers = 0;

  int totalSlot = 0;
  int runningSlot = 0;
  int idleSlot = 0;

  for(WorkerResource eachWorker: workers.values()) {
    if(eachWorker.getWorkerStatus() == WorkerStatus.LIVE) {
      numLiveWorkers++;
      idleSlot += eachWorker.getAvaliableSlots();
      totalSlot += eachWorker.getSlots();
      runningSlot += eachWorker.getUsedSlots();
    } else if(eachWorker.getWorkerStatus() == WorkerStatus.DEAD) {
      numDeadWorkers++;
    } else if(eachWorker.getWorkerStatus() == WorkerStatus.DECOMMISSION) {
      numDecommissionWorkers++;
    }
  }

  Collection<QueryInProgress> runningQueries = master.getContext().getQueryJobManager().getRunningQueries();
  Collection<QueryInProgress> finishedQueries = master.getContext().getQueryJobManager().getFinishedQueries();

  int avgQueryTime = 0;
  int minQueryTime = Integer.MAX_VALUE;
  int maxQueryTime = 0;

  long totalTime = 0;
  for(QueryInProgress eachQuery: finishedQueries) {
    int runTime = (int)(eachQuery.getQueryInfo().getFinishTime() == 0 ? -1 :
            eachQuery.getQueryInfo().getFinishTime() - eachQuery.getQueryInfo().getStartTime());
    if(runTime > 0) {
      totalTime += runTime;

      if(runTime < minQueryTime) {
        minQueryTime = runTime;
      }

      if(runTime > maxQueryTime) {
        maxQueryTime = runTime;
      }
    }
  }

  if(minQueryTime == Integer.MAX_VALUE) {
    minQueryTime = 0;
  }
  if(finishedQueries.size() > 0) {
    avgQueryTime = (int)(totalTime / (long)finishedQueries.size());
  }
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Tajo</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Master: <%=master.getMasterName()%></h2>
  <hr/>
  <h3>Master Status</h3>
  <table border='0'>
    <tr><td width='150'>Version:</td><td><%=master.getVersion()%></td></tr>
    <tr><td width='150'>Started:</td><td><%=new Date(master.getStartTime())%></td></tr>
    <tr><td width='150'>File System:</td><td><%=master.getContext().getConf().get(FileSystem.FS_DEFAULT_NAME_KEY)%></td></tr>
    <tr><td width='150'>Root dir:</td><td><%=TajoConf.getTajoRootDir(master.getContext().getConf())%></td></tr>
    <tr><td width='150'>System dir:</td><td><%=TajoConf.getSystemDir(master.getContext().getConf())%></td></tr>
    <tr><td width='150'>Warehouse dir:</td><td><%=TajoConf.getWarehouseDir(master.getContext().getConf())%></td></tr>
    <tr><td width='150'>Staging dir:</td><td><%=TajoConf.getStagingDir(master.getContext().getConf())%></td></tr>
    <tr><td width='150'>Client Service:</td><td><%=NetUtils.normalizeInetSocketAddress(master.getTajoMasterClientService().getBindAddress())%></td></tr>
    <tr><td width='150'>Catalog Service:</td><td><%=master.getCatalogServer().getCatalogServerName()%></td></tr>
    <tr><td width='150'>MaxHeap: </td><td><%=Runtime.getRuntime().maxMemory()/1024/1024%> MB</td>
    <tr><td width='150'>TotalHeap: </td><td><%=Runtime.getRuntime().totalMemory()/1024/1024%> MB</td>
    <tr><td width='150'>FreeHeap: </td><td><%=Runtime.getRuntime().freeMemory()/1024/1024%> MB</td>
    <tr><td width='150'>Configuration:</td><td><a href='conf.jsp'>detail...</a></td></tr>
    <tr><td width='150'>Environment:</td><td><a href='env.jsp'>detail...</a></td></tr>
    <tr><td width='150'>Threads:</td><td><a href='thread.jsp'>thread dump...</a></tr>
  </table>
  <hr/>

  <h3>Cluster Summary</h3>
  <table border='0' width="100%">
    <tr>
      <td width='150'><a href='cluster.jsp'>Workers:</a></td><td>Total: <%=workers.size()%>&nbsp;&nbsp;&nbsp;&nbsp;Live: <%=numLiveWorkers%>&nbsp;&nbsp;&nbsp;&nbsp;Dead: <%=numDeadWorkers%></td>
    </tr>
    <tr>
      <td width='150'>Task Slots</td><td>Total: <%=totalSlot%>&nbsp;&nbsp;&nbsp;&nbsp;Occupied:<%=runningSlot%>&nbsp;&nbsp;&nbsp;&nbsp;Idle: <%=idleSlot%></td>
    </tr>
  </table>
  <hr/>

  <h3>Query Summary</h3>
  <table border='0'>
    <tr>
      <td width="100">Queries:</td><td>Running: <%=runningQueries.size()%>&nbsp;&nbsp;&nbsp;&nbsp;Finished: <%=finishedQueries.size()%></td>
    </tr>
    <tr>
      <td width="100">Running time:</td><td>Average: <%=avgQueryTime/1000%>&nbsp;&nbsp;&nbsp;&nbsp;Min: <%=minQueryTime/1000%>&nbsp;&nbsp;&nbsp;&nbsp;Max: <%=maxQueryTime/1000%> ms</td>
    </tr>
  </table>
</div>
</body>
</html>
