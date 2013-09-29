<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="java.util.*" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="java.net.InetAddress"  %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.*" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.master.rm.*" %>
<%@ page import="org.apache.tajo.catalog.*" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryInProgress" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryMasterTask" %>
<%@ page import="org.apache.tajo.master.querymaster.Query" %>
<%@ page import="java.text.DecimalFormat" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>tajo worker</title>
<%
  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  Collection<QueryMasterTask> queryMasterTasks = tajoWorker.getWorkerContext()
          .getTajoWorkerManagerService().getQueryMaster().getQueryMasterTasks();

  Collection<QueryMasterTask> finishedQueryMasterTasks = tajoWorker.getWorkerContext()
          .getTajoWorkerManagerService().getQueryMaster().getFinishedQueryMasterTasks();

  List<TaskRunner> taskRunners = new ArrayList<TaskRunner>(tajoWorker.getWorkerContext().getTaskRunnerManager().getTaskRunners());
  List<TaskRunner> finishedTaskRunners = new ArrayList<TaskRunner>(tajoWorker.getWorkerContext().getTaskRunnerManager().getFinishedTaskRunners());

  WorkerJSPUtil.sortTaskRunner(taskRunners);
  WorkerJSPUtil.sortTaskRunner(finishedTaskRunners);

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  String tajoMasterHttp = "http://" + WorkerJSPUtil.getTajoMasterHttpAddr(tajoWorker.getConfig());
%>
</head>
<body>
<a href='<%=tajoMasterHttp%>'><img src='/static/img/tajo_logo.png'/></a>

<h3><%=tajoWorker.getWorkerContext().getWorkerName()%></h3>
<hr/>

<table border=0>
  <tr><td width='100'>MaxHeap: </td><td><%=Runtime.getRuntime().maxMemory()/1024/1024%> MB</td>
  <tr><td width='100'>TotalHeap: </td><td><%=Runtime.getRuntime().totalMemory()/1024/1024%> MB</td>
  <tr><td width='100'>FreeHeap: </td><td><%=Runtime.getRuntime().freeMemory()/1024/1024%> MB</td>
</table>
<hr/>

<h3>Running QueryMaster</h3>
<table>
  <tr><th>QueryId</th><th>StartTime</th><th>FinishTime</th><th>Progress</th><th>RunTime</th></tr>
  <%
    for(QueryMasterTask eachQueryMasterTask: queryMasterTasks) {
      Query query = eachQueryMasterTask.getQuery();
  %>
  <tr>
    <td><a href='querydetail.jsp?queryId=<%=query.getId()%>'><%=query.getId()%></a></td>
    <td><%=df.format(query.getStartTime())%></td>
    <td><%=query.getFinishTime() == 0 ? "-" : df.format(query.getFinishTime())%></td>
    <td><%=(int)(query.getProgress()*100.0f)%>%</td>
    <td><%=WorkerJSPUtil.getElapsedTime(query.getStartTime(), query.getFinishTime())%></td>
  </tr>
  <%
    }
  %>
</table>
<hr/>
<h3>Finished QueryMaster</h3>
<table>
  <tr><th>QueryId</th><th>StartTime</th><th>FinishTime</th><th>Progress</th><th>RunTime</th></tr>
  <%
    for(QueryMasterTask eachQueryMasterTask: finishedQueryMasterTasks) {
      Query query = eachQueryMasterTask.getQuery();
  %>
  <tr>
    <td><a href='querydetail.jsp?queryId=<%=query.getId()%>'><%=query.getId()%></a></td>
    <td><%=df.format(query.getStartTime())%></td>
    <td><%=query.getFinishTime() == 0 ? "-" : df.format(query.getFinishTime())%></td>
    <td><%=(int)(query.getProgress()*100.0f)%>%</td>
    <td><%=WorkerJSPUtil.getElapsedTime(query.getStartTime(), query.getFinishTime())%></td>
  </tr>
  <%
    }
  %>
</table>

<hr/>
<h3>Running Tasks</h3>
<table>
  <tr><th>TaskId</th><th>StartTime</th><th>FinishTime</th><th>RunTime</th><th>Status</th></tr>
  <%
    for(TaskRunner eachTaskRunner: taskRunners) {
  %>
  <tr>
    <td><%=eachTaskRunner.getId()%></td>
    <td><%=df.format(eachTaskRunner.getStartTime())%></td>
    <td><%=eachTaskRunner.getFinishTime() == 0 ? "-" : df.format(eachTaskRunner.getFinishTime())%></td>
    <td><%=WorkerJSPUtil.getElapsedTime(eachTaskRunner.getStartTime(), eachTaskRunner.getFinishTime())%></td>
    <td><%=eachTaskRunner.getServiceState()%></td>
      <%
  }
%>
</table>
<hr/>
<h3>Finished Tasks</h3>
<table>
  <tr><th>TaskId</th><th>StartTime</th><th>FinishTime</th><th>RunTime</th><th>Status</th></tr>
  <%
    for(TaskRunner eachTaskRunner: finishedTaskRunners) {
  %>
  <tr>
    <td><%=eachTaskRunner.getId()%></td>
    <td><%=df.format(eachTaskRunner.getStartTime())%></td>
    <td><%=eachTaskRunner.getFinishTime() == 0 ? "-" : df.format(eachTaskRunner.getFinishTime())%></td>
    <td><%=WorkerJSPUtil.getElapsedTime(eachTaskRunner.getStartTime(), eachTaskRunner.getFinishTime())%></td>
    <td><%=eachTaskRunner.getServiceState()%></td>
<%
  }
%>
</table>
</body>
</html>