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
<%@ page import="org.apache.tajo.ipc.QueryCoordinatorProtocol" %>
<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.master.rm.Worker" %>
<%@ page import="org.apache.tajo.master.rm.WorkerState" %>
<%@ page import="org.apache.tajo.service.ServiceTracker" %>
<%@ page import="org.apache.tajo.service.TajoMasterInfo" %>
<%@ page import="org.apache.tajo.storage.TablespaceManager" %>
<%@ page import="org.apache.tajo.storage.Tablespace" %>
<%@ page import="org.apache.tajo.util.NetUtils" %>
<%@ page import="org.apache.tajo.util.TUtil" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="java.util.Date" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Map" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  String[] masterName = master.getMasterName().split(":");
  InetSocketAddress socketAddress = new InetSocketAddress(masterName[0], Integer.parseInt(masterName[1]));
  String masterLabel = socketAddress.getAddress().getHostName()+ ":" + socketAddress.getPort();

  Map<Integer, Worker> workers = master.getContext().getResourceManager().getWorkers();
  Map<Integer, Worker> inactiveWorkers = master.getContext().getResourceManager().getInactiveWorkers();

  int numWorkers = 0;
  int numLiveWorkers = 0;
  int numDeadWorkers = 0;
  int numDecommissionWorkers = 0;

  int numQueryMasters = 0;
  int numLiveQueryMasters = 0;
  int numDeadQueryMasters = 0;
  int runningQueryMasterTask = 0;


  QueryCoordinatorProtocol.ClusterResourceSummary clusterResourceSummary =
          master.getContext().getResourceManager().getClusterResourceSummary();

  for(Worker eachWorker: workers.values()) {
    numQueryMasters++;
    numLiveQueryMasters++;
    runningQueryMasterTask += eachWorker.getResource().getNumQueryMasterTasks();
    numWorkers++;
    numLiveWorkers++;
  }

  for (Worker eachWorker : inactiveWorkers.values()) {
    if (eachWorker.getState() == WorkerState.LOST) {
      numQueryMasters++;
      numDeadQueryMasters++;
      numWorkers++;
      numDeadWorkers++;
    } else if(eachWorker.getState() == WorkerState.DECOMMISSIONED) {
      numDecommissionWorkers++;
    }
  }

  String numDeadWorkersHtml = numDeadWorkers == 0 ? "0" : "<font color='red'>" + numDeadWorkers + "</font>";
  String numDeadQueryMastersHtml = numDeadQueryMasters == 0 ? "0" : "<font color='red'>" + numDeadQueryMasters + "</font>";

  ServiceTracker haService = master.getContext().getHAService();
  List<TajoMasterInfo> masters = TUtil.newList();

  String activeLabel = "";
  if (haService != null) {
    if (haService.isActiveMaster()) {
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
  <h2>Tajo Master: <%=masterLabel%> <%=activeLabel%></h2>
  <hr/>
  <h3>Master Status</h3>
  <table border='0'>
    <tr><td width='150'>Version:</td><td><%=master.getVersion()%></td></tr>
    <tr><td width='150'>Started:</td><td><%=new Date(master.getStartTime())%></td></tr>
    <tr><td width='150'>File System:</td><td><%=master.getContext().getConf().get(FileSystem.FS_DEFAULT_NAME_KEY)%></td></tr>
    <tr><td width='150'>Root dir:</td><td><%=TajoConf.getTajoRootDir(master.getContext().getConf())%></td></tr>
    <tr><td width='150'>System dir:</td><td><%=TajoConf.getSystemDir(master.getContext().getConf())%></td></tr>
    <tr><td width='150'>Warehouse dir:</td><td><%=TajoConf.getWarehouseDir(master.getContext().getConf())%></td></tr>
    <tr><td width='150'>Staging dir:</td><td><%=TajoConf.getDefaultRootStagingDir(master.getContext().getConf())%></td></tr>
    <tr><td width='150'>Client Service:</td><td><%=NetUtils.normalizeInetSocketAddress(master.getTajoMasterClientService().getBindAddress())%></td></tr>
    <tr><td width='150'>Catalog Service:</td><td><%=master.getCatalogServer().getCatalogServerName()%></td></tr>
    <tr><td width='150'>Heap(Free/Total/Max): </td><td><%=Runtime.getRuntime().freeMemory()/1024/1024%> MB / <%=Runtime.getRuntime().totalMemory()/1024/1024%> MB / <%=Runtime.getRuntime().maxMemory()/1024/1024%> MB</td>
    <tr><td width='150'>Configuration:</td><td><a href='conf.jsp'>detail...</a></td></tr>
    <tr><td width='150'>Environment:</td><td><a href='env.jsp'>detail...</a></td></tr>
    <tr><td width='150'>Threads:</td><td><a href='thread.jsp'>thread dump...</a></tr>
  </table>
  <hr/>
  <h3>Tablespaces</h3>
  <table width="100%" class="border_table" border="1">
    <tr><th>Tablespace Name</th><th>URI</th><th>Handler</th></tr>
    <% for (Tablespace space : TablespaceManager.getAllTablespaces()) {
      if (space.isVisible()) { %>
    <tr><td><%=space.getName()%></td><td><%=space.getUri()%></td><td><%=space.getClass().getName()%></td></tr>
    <% }}%>
  </table>
  <hr/>
  <h3>Cluster Summary</h3>
  <table width="100%" class="border_table" border="1">
    <tr><th>Type</th><th>Total</th><th>Live</th><th>Dead</th><th>Running Master</th><th>Memory Resource<br/>(used/total)</th><th>Disk Resource<br/>(used/total)</th></tr>
    <tr>
      <td><a href='cluster.jsp'>Query Master</a></td>
      <td align='right'><%=numQueryMasters%></td>
      <td align='right'><%=numLiveQueryMasters%></td>
      <td align='right'><%=numDeadQueryMastersHtml%></td>
      <td align='right'><%=runningQueryMasterTask%></td>
      <td align='center'>-</td>
      <td align='center'>-</td>
    </tr>
    <tr>
      <td><a href='cluster.jsp'>Worker</a></td>
      <td align='right'><%=numWorkers%></td>
      <td align='right'><%=numLiveWorkers%></td>
      <td align='right'><%=numDeadWorkersHtml%></td>
      <td align='right'>-</td>
      <td align='center'><%=clusterResourceSummary.getTotalMemoryMB() - clusterResourceSummary.getTotalAvailableMemoryMB()%>/<%=clusterResourceSummary.getTotalMemoryMB()%></td>
      <td align='center'><%=clusterResourceSummary.getTotalDiskSlots() - clusterResourceSummary.getTotalAvailableDiskSlots()%>/<%=clusterResourceSummary.getTotalDiskSlots()%></td>
    </tr>
<%
    if (haService != null) {
%>
    <tr>
      <td><a href='cluster.jsp'>Masters</a></td>
      <td align='right'><%=haService.getMasters().size()%></td>
      <td align='right'><%=numLiveMasters%></td>
      <td align='right'><%=numDeadMasters%></td>
      <td align='right'>-</td>
      <td align='center'>-</td>
      <td align='center'>-</td>
    </tr>
<%
    }
%>
  </table>
  <p/>
  <hr/>

  <h3>Query Summary</h3>
  <table width="100%" class="border_table" border="1">
    <tr><th>Running Queries</th><th>Finished Queries</th><th>Average Execution Time</th><th>Min. Execution Time</th><th>Max. Execution Time</th></tr>
    <tr>
      <td align='right'><%=master.getContext().getQueryJobManager().getRunningQueries().size()%></td>
      <td align='right'><%=master.getContext().getQueryJobManager().getExecutedQuerySize() %></td>
      <td align='left'><%=master.getContext().getQueryJobManager().getAvgExecutionTime()/1000%> sec</td>
      <td align='left'><%=master.getContext().getQueryJobManager().getMinExecutionTime()/1000%> sec</td>
      <td align='left'><%=master.getContext().getQueryJobManager().getMaxExecutionTime()/1000%> sec</td>
    </tr>
  </table>
</div>
</body>
</html>
