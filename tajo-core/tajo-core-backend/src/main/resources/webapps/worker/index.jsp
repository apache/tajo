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

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "./style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>tajo main</title>
<%
  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  Collection<QueryMasterTask> queryMasterTasks = tajoWorker.getWorkerContext()
          .getTajoWorkerManagerService().getQueryMaster().getQueryMasterTasks();
%>
</head>
<body>
<img src='img/tajo_logo.png'/>

<h3><%=tajoWorker.getWorkerContext().getWorkerName()%></h3>
<hr/>

<table border=0>
  <tr><td width='100'>MaxHeap: </td><td><%=Runtime.getRuntime().maxMemory()/1024/1024%> MB</td>
  <tr><td width='100'>TotalHeap: </td><td><%=Runtime.getRuntime().totalMemory()/1024/1024%> MB</td>
  <tr><td width='100'>FreeHeap: </td><td><%=Runtime.getRuntime().freeMemory()/1024/1024%> MB</td>
</table>

<h3>QueryMaster</h3>
<table>

<%
  for(QueryMasterTask eachQueryMasterTask: queryMasterTasks) {
    Query query = eachQueryMasterTask.getQuery();
%>
    <tr>
      <td><a href='querydetail.jsp?queryId=<%=query.getId()%>'><%=query.getId()%></a></td>
      <td><%=query.getFinishTime()%></td>
      <td><%=query.getStartTime()%></td>
      <td><%=query.getProgress()%></td>
    </tr>
<%
  }
%>
</table>
</body>
</html>