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
<%@ page import="org.apache.tajo.master.rm.NodeStatus" %>
<%@ page import="org.apache.tajo.master.rm.NodeState" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.TUtil" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.util.*" %>
<%@ page import="java.net.InetSocketAddress" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  String[] masterName = master.getMasterName().split(":");
  InetSocketAddress socketAddress = new InetSocketAddress(masterName[0], Integer.parseInt(masterName[1]));
  String masterLabel = socketAddress.getAddress().getHostName()+ ":" + socketAddress.getPort();

  Map<Integer, NodeStatus> nodes = master.getContext().getResourceManager().getNodes();
  List<Integer> wokerKeys = new ArrayList<>(nodes.keySet());
  Collections.sort(wokerKeys);

  int runningQueryMasterTasks = 0;

  Set<NodeStatus> liveNodes = new TreeSet<>();
  Set<NodeStatus> deadNodes = new TreeSet<>();
  Set<NodeStatus> decommissionNodes = new TreeSet<>();

  Set<NodeStatus> liveQueryMasters = new TreeSet<>();
  Set<NodeStatus> deadQueryMasters = new TreeSet<>();

  for(NodeStatus eachNode: nodes.values()) {
    liveQueryMasters.add(eachNode);
    liveNodes.add(eachNode);
    runningQueryMasterTasks += eachNode.getNumRunningQueryMaster();
  }

  for (NodeStatus inactiveNode : master.getContext().getResourceManager().getInactiveNodes().values()) {
    NodeState state = inactiveNode.getState();

    if (state == NodeState.LOST) {
      deadQueryMasters.add(inactiveNode);
      deadNodes.add(inactiveNode);
    } else if (state == NodeState.DECOMMISSIONED) {
      decommissionNodes.add(inactiveNode);
    }
  }

  String deadNodesHtml = deadNodes.isEmpty() ? "0": "<font color='red'>" + deadNodes.size() + "</font>";
  String deadQueryMastersHtml = deadQueryMasters.isEmpty() ? "0": "<font color='red'>" + deadQueryMasters.size() + "</font>";

  ServiceTracker haService = master.getContext().getHAService();
  List<TajoMasterInfo> masters = new ArrayList<>();

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
  <h2>Tajo Master: <%=masterLabel%> <%=activeLabel%></h2>
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
    <tr><th>No</th><th>QueryMaster</th><th>Client Port</th><th>Running Query</th><th>Heartbeat</th><th>Status</th></tr>

<%
    int no = 1;
    for(NodeStatus queryMaster: liveQueryMasters) {
        WorkerConnectionInfo connectionInfo = queryMaster.getConnectionInfo();
        String queryMasterHttp = "http://" + connectionInfo.getHost()
                + ":" + connectionInfo.getHttpInfoPort() + "/index.jsp";
%>
    <tr>
        <td width='30' align='right'><%=no++%></td>
        <td><a href='<%=queryMasterHttp%>'><%=connectionInfo.getHost() + ":" + connectionInfo.getQueryMasterPort()%></a></td>
        <td width='100' align='center'><%=connectionInfo.getClientPort()%></td>
        <td width='200' align='right'><%=queryMaster.getNumRunningQueryMaster()%></td>
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
      for(NodeStatus queryMaster: deadQueryMasters) {
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
  <h2>Node</h2>
  <div>Live:<%=liveNodes.size()%>, Dead: <%=deadNodesHtml%></div>
  <hr/>
  <h3>Live Nodes</h3>
<%
  if(liveNodes.isEmpty()) {
    out.write("No Live Nodes\n");
  } else {
%>
  <table width="100%" class="border_table" border="1">
    <tr><th>No</th><th>Node</th><th>PullServer<br/>Port</th><th>Running Tasks</th><th>Available</th><th>Total</th><th>Heartbeat</th><th>Status</th></tr>
<%
    int no = 1;
    for(NodeStatus node: liveNodes) {
        WorkerConnectionInfo connectionInfo = node.getConnectionInfo();
        String nodeHttp = "http://" + connectionInfo.getHost() + ":" + connectionInfo.getHttpInfoPort() + "/index.jsp";
%>
    <tr>
        <td width='30' align='right'><%=no++%></td>
        <td><a href='<%=nodeHttp%>'><%=connectionInfo.getHostAndPeerRpcPort()%></a></td>
        <td width='80' align='center'><%=connectionInfo.getPullServerPort()%></td>
        <td width='100' align='right'><%=node.getNumRunningTasks()%></td>
        <td width='150' align='center'><%=node.getAvailableResource()%></td>
        <td width='150' align='center'><%=node.getTotalResourceCapability()%></td>
        <td width='100' align='right'><%=JSPUtil.getElapsedTime(node.getLastHeartbeatTime(), System.currentTimeMillis())%></td>
        <td width='100' align='center'><%=node.getState()%></td>
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
  <h3>Dead Nodes</h3>

<%
    if(deadNodes.isEmpty()) {
%>
  No Dead Nodes
<%
  } else {
%>
  <table width="300" class="border_table" border="1">
    <tr><th>No</th><th>Node</th></tr>
<%
      int no = 1;
      for(NodeStatus node: deadNodes) {
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><%=node.getConnectionInfo().getHostAndPeerRpcPort()%></td>
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
