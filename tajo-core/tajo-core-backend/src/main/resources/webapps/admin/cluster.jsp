<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.master.rm.*" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  Map<String, WorkerResource> workers = master.getContext().getResourceManager().getWorkers();
  List<String> wokerKeys = new ArrayList<String>(workers.keySet());
  Collections.sort(wokerKeys);

  int totalSlot = 0;
  int runningSlot = 0;
  int idleSlot = 0;

  List<WorkerResource> liveWorkers = new ArrayList<WorkerResource>();
  List<WorkerResource> deadWorkers = new ArrayList<WorkerResource>();
  List<WorkerResource> decommissionWorkers = new ArrayList<WorkerResource>();

  for(WorkerResource eachWorker: workers.values()) {
    if(eachWorker.getWorkerStatus() == WorkerStatus.LIVE) {
      liveWorkers.add(eachWorker);
      idleSlot += eachWorker.getAvaliableSlots();
      totalSlot += eachWorker.getSlots();
      runningSlot += eachWorker.getUsedSlots();
    } else if(eachWorker.getWorkerStatus() == WorkerStatus.DEAD) {
      deadWorkers.add(eachWorker);
    } else if(eachWorker.getWorkerStatus() == WorkerStatus.DECOMMISSION) {
      decommissionWorkers.add(eachWorker);
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
  <h2>Tajo Master: <%=master.getMasterName()%></h2>
  <hr/>
  <div>Live:<%=liveWorkers.size()%>, Dead: <%=deadWorkers.size()%>, Decommission: <%=decommissionWorkers.size()%></div>
  <hr/>
  <h3>Live Workers</h3>
<%
  if(liveWorkers.isEmpty()) {
%>
    No Live Workers
<%
  } else {
%>
  <table width="100%" class="border_table" border="1">
    <tr><th>No</th><th>Worker</th><th>Ports</th><th>Running Tasks</th><th>Slot</th></th><th>Heap(free/max)</th><th>Disk</th><th>Cpu</th><th>Status</th></tr>
<%
    int no = 1;
    for(WorkerResource worker: liveWorkers) {
          String workerHttp = "http://" + worker.getAllocatedHost() + ":" + worker.getHttpPort();
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><a href='<%=workerHttp%>'><%=worker.getAllocatedHost() + ":" + worker.getPeerRpcPort()%></a></td>
      <td width='100'><%=worker.portsToStr()%></td>
      <td width='100' align='right'><%=worker.getNumRunningTasks()%></td>
      <td width='100' align='right'><%=worker.getUsedSlots()%>/<%=worker.getSlots()%></td>
      <td width='100' align='left'><%=worker.getFreeHeap()/1024/1024%>/<%=worker.getMaxHeap()/1024/1024%> MB</td>
      <td width='100' align='right'><%=worker.getUsedDiskSlots()%>/<%=worker.getDiskSlots()%></td>
      <td width='100' align='right'><%=worker.getUsedCpuCoreSlots()%>/<%=worker.getCpuCoreSlots()%></td>
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
    <tr><th>No</th><th>Worker</th><th>Ports</th><th>Running Tasks</th><th>Slot</th></th><th>Heap(free/max)</th><th>Disk</th><th>Cpu</th><th>Status</th></tr>
<%
      int no = 1;
      for(WorkerResource worker: deadWorkers) {
%>
    <tr>
      <td width='30' align='right'><%=no++%></td>
      <td><%=worker.getAllocatedHost() + ":" + worker.getPeerRpcPort()%></td>
      <td><%=worker.portsToStr()%></td>
      <td><%=worker.getNumRunningTasks()%></td>
      <td><%=worker.getUsedSlots()%>/<%=worker.getSlots()%></td>
      <td><%=worker.getFreeHeap()/1024/1024%>/<%=worker.getMaxHeap()/1024/1024%> MB</td>
      <td><%=worker.getUsedDiskSlots()%>/<%=worker.getDiskSlots()%></td>
      <td><%=worker.getUsedCpuCoreSlots()%>/<%=worker.getCpuCoreSlots()%></td>
      <td><%=worker.getWorkerStatus()%></td>
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
