<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="java.util.*" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="java.net.InetAddress"  %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.master.rm.*" %>
<%@ page import="org.apache.tajo.catalog.*" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryInProgress" %>
<%@ page import="java.text.SimpleDateFormat" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>tajo main</title>
  <%
    TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
    CatalogService catalog = master.getCatalog();
    Map<String, WorkerResource> workers = master.getContext().getResourceManager().getWorkers();
    List<String> wokerKeys = new ArrayList<String>(workers.keySet());
    Collections.sort(wokerKeys);
  %>
</head>
<body>
<img src='/static/img/tajo_logo.png'/>
<hr/>

<h3>Works</h3>
<div>Live:<%=wokerKeys.size()%></div>
<table>
  <tr><th>Worker</th><th>Ports</th><th>Running Tasks</th><th>Slot</th></th><th>Heap(free/max)</th><th>Disk</th><th>Cpu</th><th>Status</th></tr>
  <%
    for(String eachWorker: wokerKeys) {
      WorkerResource worker = workers.get(eachWorker);
      String workerHttp = "http://" + worker.getAllocatedHost() + ":" + worker.getHttpPort();
  %>

  <tr>
    <td><a href='<%=workerHttp%>'><%=eachWorker%></a></td>
    <td><%=worker.portsToStr()%></td>
    <td><%=worker.getNumRunningTasks()%></td>
    <td><%=worker.getUsedSlots()%>/<%=worker.getSlots()%></td>
    <td><%=worker.getFreeHeap()/1024/1024%>/<%=worker.getMaxHeap()/1024/1024%> MB</td>
    <td><%=worker.getUsedDiskSlots()%>/<%=worker.getDiskSlots()%></td>
    <td><%=worker.getUsedCpuCoreSlots()%>/<%=worker.getCpuCoreSlots()%></td>
    <td><%=worker.getWorkerStatus()%></td>
  </tr>
  <%
    }

    if(workers.isEmpty()) {
  %>
  <tr>
    <td colspan='7'>No Workers</td>
  </tr>
  <%
    }
  %>
</table>

<%
  Collection<QueryInProgress> runningQueries = master.getContext().getQueryJobManager().getRunningQueries();
  Collection<QueryInProgress> finishedQueries = master.getContext().getQueryJobManager().getFinishedQueries();
%>
<hr/>
<h3>Running Queries</h3>
<table>
  <tr></tr><th>QueryId</th><th>Query Master</th><th>Started</th><th>Progress</th><th>Time</th><th>sql</th></tr>
<%
  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  for(QueryInProgress eachQuery: runningQueries) {
    long time = System.currentTimeMillis() - eachQuery.getQueryInfo().getStartTime();
%>
    <tr>
      <td><%=eachQuery.getQueryId()%></td>
      <td><%=eachQuery.getQueryInfo().getQueryMasterHost()%></td>
      <td><%=df.format(eachQuery.getQueryInfo().getStartTime())%></td>
      <td><%=(int)(eachQuery.getQueryInfo().getProgress() * 100.0f)%>%</td>
      <td><%=(int)(time/1000)%> sec</td>
      <td><%=eachQuery.getQueryInfo().getSql()%></td>
    </tr>
<%
  }
%>
</table>

<hr/>
<h3>Finished Queries</h3>
<table>
  <tr></tr><th>QueryId</th><th>Query Master</th><th>Started</th><th>Finished</th><th>Time</th><th>Status</th><th>sql</th></tr>
  <%
    for(QueryInProgress eachQuery: finishedQueries) {
      long runTime = eachQuery.getQueryInfo().getFinishTime() == 0 ? -1 :
              eachQuery.getQueryInfo().getFinishTime() - eachQuery.getQueryInfo().getStartTime();
  %>
  <tr>
    <td><%=eachQuery.getQueryId()%></td>
    <td><%=eachQuery.getQueryInfo().getQueryMasterHost()%></td>
    <td><%=df.format(eachQuery.getQueryInfo().getStartTime())%></td>
    <td><%=df.format(eachQuery.getQueryInfo().getFinishTime())%></td>
    <td><%=runTime%> ms</td>
    <td><%=eachQuery.getQueryInfo().getQueryState()%></td>
    <td><%=eachQuery.getQueryInfo().getSql()%></td>
  </tr>
  <%
    }
  %>
</table>
</body>
</html>
