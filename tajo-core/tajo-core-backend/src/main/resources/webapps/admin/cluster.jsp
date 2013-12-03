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
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.master.rm.*" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  Map<String, WorkerResource> workers = master.getContext().getResourceManager().getWorkers();
  List<String> wokerKeys = new ArrayList<String>(workers.keySet());
  Collections.sort(wokerKeys);

  int runningQueryMasterTasks = 0;

  Set<WorkerResource> liveWorkers = new TreeSet<WorkerResource>();
  Set<WorkerResource> deadWorkers = new TreeSet<WorkerResource>();
  Set<WorkerResource> decommissionWorkers = new TreeSet<WorkerResource>();

  Set<WorkerResource> liveQueryMasters = new TreeSet<WorkerResource>();
  Set<WorkerResource> deadQueryMasters = new TreeSet<WorkerResource>();

  for(WorkerResource eachWorker: workers.values()) {
    if(eachWorker.isQueryMasterMode()) {
      if(eachWorker.getWorkerStatus() == WorkerStatus.LIVE) {
        liveQueryMasters.add(eachWorker);
        runningQueryMasterTasks += eachWorker.getNumQueryMasterTasks();
      }
      if(eachWorker.getWorkerStatus() == WorkerStatus.DEAD) {
        deadQueryMasters.add(eachWorker);
      }
    }

    if(eachWorker.isTaskRunnerMode()) {
      if(eachWorker.getWorkerStatus() == WorkerStatus.LIVE) {
        liveWorkers.add(eachWorker);
      } else if(eachWorker.getWorkerStatus() == WorkerStatus.DEAD) {
        deadWorkers.add(eachWorker);
      } else if(eachWorker.getWorkerStatus() == WorkerStatus.DECOMMISSION) {
        decommissionWorkers.add(eachWorker);
      }
    }
  }

  String deadWorkersHtml = deadWorkers.isEmpty() ? "0": "<font color='red'>" + deadWorkers.size() + "</font>";
  String deadQueryMastersHtml = deadQueryMasters.isEmpty() ? "0": "<font color='red'>" + deadQueryMasters.size() + "</font>";
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
  <h2>Query Master</h2>
  <div>Live:<%=liveQueryMasters.size()%>, Dead: <%=deadQueryMastersHtml%>, QueryMaster Tasks: <%=runningQueryMasterTasks%></div>
  <h3>Live QueryMasters</h3>
<%
  if(liveQueryMasters.isEmpty()) {
    out.write("No Live QueryMasters\n");
  } else {
%>
  <table width="100%" class="border_table" border="1">
    <tr><th>No</th><th>QueryMaster</th><th>Client Port</th><th>Running Query</th><th>Heap(free/max)</th><th>Heartbeat</th><th>Status</th></tr>

<%
    int no = 1;
    for(WorkerResource queryMaster: liveQueryMasters) {
          String queryMasterHttp = "http://" + queryMaster.getAllocatedHost() + ":" + queryMaster.getHttpPort() + "/index.jsp";
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><a href='<%=queryMasterHttp%>'><%=queryMaster.getAllocatedHost() + ":" + queryMaster.getQueryMasterPort()%></a></td>
      <td width='100' align='center'><%=queryMaster.getClientPort()%></td>
      <td width='200' align='right'><%=queryMaster.getNumQueryMasterTasks()%></td>
      <td width='200' align='center'><%=queryMaster.getFreeHeap()/1024/1024%>/<%=queryMaster.getMaxHeap()/1024/1024%> MB</td>
      <td width='100' align='right'><%=JSPUtil.getElapsedTime(queryMaster.getLastHeartbeat(), System.currentTimeMillis())%></td>
      <td width='100' align='center'><%=queryMaster.getWorkerStatus()%></td>
    </tr>
<%
    } //end fo for
%>
  </table>
<%
    } //end of if
%>

  <p/>

<%
  if(!deadQueryMasters.isEmpty()) {
%>
  <hr/>
  <h3>Dead QueryMaster</h3>
  <table width="100%" class="border_table" border="1">
    <tr><th>No</th><th>QueryMaster</th><th>Client Port</th><th>Status</th></tr>
<%
      int no = 1;
      for(WorkerResource queryMaster: deadQueryMasters) {
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><%=queryMaster.getAllocatedHost() + ":" + queryMaster.getQueryMasterPort()%></td>
      <td><%=queryMaster.getClientPort()%></td>
      <td align='center'><%=queryMaster.getWorkerStatus()%></td>
    </tr>
<%
      } //end fo for
%>
  </table>
  <p/>
<%
    } //end of if
%>

  <hr/>
  <h2>Worker</h2>
  <div>Live:<%=liveWorkers.size()%>, Dead: <%=deadWorkersHtml%></div>
  <hr/>
  <h3>Live Workers</h3>
<%
  if(liveWorkers.isEmpty()) {
    out.write("No Live Workers\n");
  } else {
%>
  <table width="100%" class="border_table" border="1">
    <tr><th>No</th><th>Worker</th><th>PullServer<br/>Port</th><th>Running Tasks</th><th>Memory Resource<br/>(used/total)</th><th>Disk Resource<br/>(used/total)</th></th><th>Heap(free/max)</th><th>Heartbeat</th><th>Status</th></tr>
<%
    int no = 1;
    for(WorkerResource worker: liveWorkers) {
          String workerHttp = "http://" + worker.getAllocatedHost() + ":" + worker.getHttpPort() + "/index.jsp";
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><a href='<%=workerHttp%>'><%=worker.getAllocatedHost() + ":" + worker.getPeerRpcPort()%></a></td>
      <td width='80' align='center'><%=worker.getPullServerPort()%></td>
      <td width='100' align='right'><%=worker.getNumRunningTasks()%></td>
      <td width='150' align='center'><%=worker.getUsedMemoryMB()%>/<%=worker.getMemoryMB()%></td>
      <td width='100' align='center'><%=worker.getUsedDiskSlots()%>/<%=worker.getDiskSlots()%></td>
      <td width='100' align='center'><%=worker.getFreeHeap()/1024/1024%>/<%=worker.getMaxHeap()/1024/1024%> MB</td>
      <td width='100' align='right'><%=JSPUtil.getElapsedTime(worker.getLastHeartbeat(), System.currentTimeMillis())%></td>
      <td width='100' align='center'><%=worker.getWorkerStatus()%></td>
    </tr>
<%
    } //end fo for
%>
    </table>
<%
  } //end of if
%>

  <p/>
  <hr/>
  <p/>
  <h3>Dead Workers</h3>

<%
    if(deadWorkers.isEmpty()) {
%>
  No Dead Workers
<%
  } else {
%>
  <table width="100%" class="border_table" border="1">
    <tr><th>No</th><th>Worker</th><th>PullServer Port</th><th>Running Tasks</th><th>Memory Resource</th><th>Disk Resource</th></th><th>Heap(free/max)</th><th>Heartbeat</th><th>Status</th></tr>
<%
      int no = 1;
      for(WorkerResource worker: deadWorkers) {
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><%=worker.getAllocatedHost() + ":" + worker.getPeerRpcPort()%></td>
      <td width='150' align='center'><%=worker.getPullServerPort()%></td>
      <td width='100' align='right'><%=worker.getUsedMemoryMB()%>/<%=worker.getMemoryMB()%></td>
      <td width='100' align='right'><%=worker.getUsedDiskSlots()%>/<%=worker.getDiskSlots()%></td>
      <td width='100' align='left'><%=worker.getFreeHeap()/1024/1024%>/<%=worker.getMaxHeap()/1024/1024%> MB</td>
      <td width='100' align='center'><%=worker.getWorkerStatus()%></td>
    </tr>
<%
      } //end fo for
%>
  </table>
<%
    } //end of if
%>
</div>
</body>
</html>
