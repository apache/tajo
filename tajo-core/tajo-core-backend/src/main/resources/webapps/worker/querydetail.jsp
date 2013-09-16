<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.QueryId" %>
<%@ page import="org.apache.tajo.master.querymaster.Query" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryMasterTask"  %>
<%@ page import="org.apache.tajo.master.querymaster.QueryUnit" %>
<%@ page import="org.apache.tajo.master.querymaster.SubQuery" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.Collection" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<html>
<head>
  <link rel="stylesheet" type="text/css" href="./style.css"/>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Query Detail Info</title>
  <%
  QueryId queryId = TajoIdUtils.parseQueryId(request.getParameter("queryId"));
  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
          .getTajoWorkerManagerService().getQueryMaster().getQueryMasterTask(queryId);

  Query query = queryMasterTask.getQuery();
  %>
  <h1><% out.write(queryId.toString()); %></h1>
  <h2>Logical Plan</h2>
  <pre>
  <%
    out.write(query.getPlan().getLogicalPlan().toString());
  %>
  </pre>
  <h2>Distributed Query Plan</h2>
  <pre>
  <%
    out.write(query.getPlan().toString());
  %>
  </pre>
  <%
  Collection<SubQuery> subQueries = query.getSubQueries();

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  for(SubQuery eachSubQuery: subQueries) {
%>
  <div><%=eachSubQuery.getId()%>(<%=eachSubQuery.getState()%>)</div>
  <div>Started:<%=df.format(eachSubQuery.getStartTime())%>, <%=eachSubQuery.getFinishTime() == 0 ? "-" : df.format(eachSubQuery.getFinishTime())%></div>
  <table>
    <tr><th>Id</th><th>Status</th><th>Start Time</th><th>Running Time</th><th>Host</th></tr>
<%
    QueryUnit[] queryUnits = eachSubQuery.getQueryUnits();
    for(QueryUnit eachQueryUnit: queryUnits) {
%>
      <tr>
        <td><%=eachQueryUnit.getId()%></td>
        <td><%=eachQueryUnit.getState()%></td>
        <td><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : df.format(eachQueryUnit.getLaunchTime())%></td>
        <td><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : eachQueryUnit.getRunningTime()%> ms</td>
        <td><%=eachQueryUnit.getSucceededHost() == null ? "-" : eachQueryUnit.getSucceededHost()%></td>
      </tr>
<%
    }
%>
  </table>
<%
  }
%>
</head>
<body>