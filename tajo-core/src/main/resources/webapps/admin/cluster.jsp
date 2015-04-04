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

<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.master.cluster.WorkerConnectionInfo" %>
<%@ page import="org.apache.tajo.service.ServiceTracker" %>
<%@ page import="org.apache.tajo.service.TajoMasterInfo" %>
<%@ page import="org.apache.tajo.master.rm.Worker" %>
<%@ page import="org.apache.tajo.master.rm.WorkerResource" %>
<%@ page import="org.apache.tajo.master.rm.WorkerState" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.TUtil" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.service.ServiceTracker" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  Map<Integer, Worker> workers = master.getContext().getResourceManager().getWorkers();
  List<Integer> wokerKeys = new ArrayList<Integer>(workers.keySet());
  Collections.sort(wokerKeys);

  int runningQueryMasterTasks = 0;

  Set<Worker> liveWorkers = new TreeSet<Worker>();
  Set<Worker> deadWorkers = new TreeSet<Worker>();
  Set<Worker> decommissionWorkers = new TreeSet<Worker>();

  Set<Worker> liveQueryMasters = new TreeSet<Worker>();
  Set<Worker> deadQueryMasters = new TreeSet<Worker>();

  for(Worker eachWorker: workers.values()) {
    liveQueryMasters.add(eachWorker);
    liveWorkers.add(eachWorker);
    runningQueryMasterTasks += eachWorker.getResource().getNumQueryMasterTasks();
  }

  for (Worker inactiveWorker : master.getContext().getResourceManager().getInactiveWorkers().values()) {
    WorkerState state = inactiveWorker.getState();

    if (state == WorkerState.LOST) {
      deadQueryMasters.add(inactiveWorker);
      deadWorkers.add(inactiveWorker);
    } else if (state == WorkerState.DECOMMISSIONED) {
      decommissionWorkers.add(inactiveWorker);
    }
  }

  String deadWorkersHtml = deadWorkers.isEmpty() ? "0": "<font color='red'>" + deadWorkers.size() + "</font>";
  String deadQueryMastersHtml = deadQueryMasters.isEmpty() ? "0": "<font color='red'>" + deadQueryMasters.size() + "</font>";

  ServiceTracker haService = master.getContext().getHAService();
  List<TajoMasterInfo> masters = TUtil.newList();

  String activeLabel = "";
  if (haService != null) {
    if (haService.isActiveStatus()) {
      activeLabel = "<font color='#1e90ff'>(active)</font>";
    } else {
      activeLabel = "<font color='#1e90ff'>(backup)</font>";
    }

    masters.addAll(haService.getMasters());
  }

  int numLiveMasters = 0;
  int numDeadMasters = 0;

  for(TajoMasterInfo eachMaster : masters) {
    if (eachMaster.isAvailable()) {
      numLiveMasters++;
    } else {
      numDeadMasters++;
    }
  }
  String deadMasterHtml = numDeadMasters == 0 ? "0": "<font color='red'>" + numDeadMasters +"</font>";

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
  <h2>Tajo Master: <%=master.getMasterName()%> <%=activeLabel%></h2>
  <div>Live:<%=numLiveMasters%>, Dead: <%=deadMasterHtml%>, Total: <%=masters.size()%></div>
<%
  if (masters != null) {
    if(numLiveMasters == 0) {
      out.write("No TajoMasters\n");
    } else {
%>
  <p/>
  <table width="100%" class="border_table" border="1">
    <tr><th>No</th><th>TajoMaster</th><th>Rpc Server</th><th>Rpc Client</th><th>ResourceTracker</th>
      <th>Catalog</th><th>Active/Backup</th><th>Status</th></tr>
    <%
      int no = 1;

      for(TajoMasterInfo eachMaster : masters) {
      String tajoMasterHttp = "http://" + eachMaster.getWebServerAddress().getHostName() + ":" +
          eachMaster.getWebServerAddress().getPort() + "/index.jsp";
      String isActive = eachMaster.isActive() == true ? "ACTIVE" : "BACKUP";
      String isAvailable = eachMaster.isAvailable() == true ? "RUNNING" : "FAILED";
    %>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><a href='<%=tajoMasterHttp%>'><%=eachMaster.getWebServerAddress().getHostName() + ":" +
          eachMaster.getWebServerAddress().getPort()%></a></td>
      <td width='200' align='right'><%=eachMaster.getTajoMasterAddress().getHostName() + ":" +
          eachMaster.getTajoMasterAddress().getPort()%></td>
      <td width='200' align='right'><%=eachMaster.getTajoClientAddress().getHostName() + ":" +
          eachMaster.getTajoClientAddress().getPort()%></td>
      <td width='200' align='right'><%=eachMaster.getWorkerResourceTrackerAddr().getHostName() + ":" +
          eachMaster.getWorkerResourceTrackerAddr().getPort()%></td>
      <td width='200' align='right'><%=eachMaster.getCatalogAddress().getHostName() + ":" +
          eachMaster.getCatalogAddress().getPort()%></td>
      <td width='200' align='right'><%=isActive%></td>
      <td width='100' align='center'><%=isAvailable%></td>
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
  } //end of if
%>

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
    <tr><th>No</th><th>QueryMaster</th><th>Client Port</th><th>Running Query</th><th>Heap(free/total/max)</th><th>Heartbeat</th><th>Status</th></tr>

<%
    int no = 1;
    for(Worker queryMaster: liveQueryMasters) {
        WorkerResource resource = queryMaster.getResource();
        WorkerConnectionInfo connectionInfo = queryMaster.getConnectionInfo();
        String queryMasterHttp = "http://" + connectionInfo.getHost()
                + ":" + connectionInfo.getHttpInfoPort() + "/index.jsp";
%>
    <tr>
        <td width='30' align='right'><%=no++%></td>
        <td><a href='<%=queryMasterHttp%>'><%=connectionInfo.getHost() + ":" + connectionInfo.getQueryMasterPort()%></a></td>
        <td width='100' align='center'><%=connectionInfo.getClientPort()%></td>
        <td width='200' align='right'><%=resource.getNumQueryMasterTasks()%></td>
        <td width='200' align='center'><%=resource.getFreeHeap()/1024/1024%>/<%=resource.getTotalHeap()/1024/1024%>/<%=resource.getMaxHeap()/1024/1024%> MB</td>
        <td width='100' align='right'><%=JSPUtil.getElapsedTime(queryMaster.getLastHeartbeatTime(), System.currentTimeMillis())%></td>
        <td width='100' align='center'><%=queryMaster.getState()%></td>
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
  <table width="300" class="border_table" border="1">
    <tr><th>No</th><th>QueryMaster</th>
<%
      int no = 1;
      for(Worker queryMaster: deadQueryMasters) {
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><%=queryMaster.getConnectionInfo().getHost() + ":" + queryMaster.getConnectionInfo().getQueryMasterPort()%></td>
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
    <tr><th>No</th><th>Worker</th><th>PullServer<br/>Port</th><th>Running Tasks</th><th>Memory Resource<br/>(used/total)</th><th>Disk Resource<br/>(used/total)</th><th>Heap<br/>(free/total/max)</th><th>Heartbeat</th><th>Status</th></tr>
<%
    int no = 1;
    for(Worker worker: liveWorkers) {
        WorkerResource resource = worker.getResource();
        WorkerConnectionInfo connectionInfo = worker.getConnectionInfo();
        String workerHttp = "http://" + connectionInfo.getHost() + ":" + connectionInfo.getHttpInfoPort() + "/index.jsp";
%>
    <tr>
        <td width='30' align='right'><%=no++%></td>
        <td><a href='<%=workerHttp%>'><%=connectionInfo.getHostAndPeerRpcPort()%></a></td>
        <td width='80' align='center'><%=connectionInfo.getPullServerPort()%></td>
        <td width='100' align='right'><%=resource.getNumRunningTasks()%></td>
        <td width='150' align='center'><%=resource.getUsedMemoryMB()%>/<%=resource.getMemoryMB()%></td>
        <td width='100' align='center'><%=resource.getUsedDiskSlots()%>/<%=resource.getDiskSlots()%></td>
        <td width='200' align='center'><%=resource.getFreeHeap()/1024/1024%>/<%=resource.getTotalHeap()/1024/1024%>/<%=resource.getMaxHeap()/1024/1024%> MB</td>
        <td width='100' align='right'><%=JSPUtil.getElapsedTime(worker.getLastHeartbeatTime(), System.currentTimeMillis())%></td>
        <td width='100' align='center'><%=worker.getState()%></td>
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
  <table width="300" class="border_table" border="1">
    <tr><th>No</th><th>Worker</th></tr>
<%
      int no = 1;
      for(Worker worker: deadWorkers) {
        WorkerResource resource = worker.getResource();
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><%=worker.getConnectionInfo().getHostAndPeerRpcPort()%></td>
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
