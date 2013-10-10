<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.master.querymaster.*" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.*" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="org.apache.tajo.QueryId" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.ExecutionBlockId" %>

<%
  QueryId queryId = TajoIdUtils.parseQueryId(request.getParameter("queryId"));
  ExecutionBlockId ebid = TajoIdUtils.createExecutionBlockId(request.getParameter("ebid"));
  String sort = request.getParameter("sort");
  if(sort == null) {
    sort = "id";
  }
  String sortOrder = request.getParameter("sortOrder");
  if(sortOrder == null) {
    sortOrder = "asc";
  }

  String nextSortOrder = "asc";
  if("asc".equals(sortOrder)) {
    nextSortOrder = "desc";
  }

  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
          .getTajoWorkerManagerService().getQueryMaster().getQueryMasterTask(queryId, true);

  if(queryMasterTask == null) {
    out.write("<script type='text/javascript'>alert('no query'); history.back(0); </script>");
    return;
  }

  Query query = queryMasterTask.getQuery();
  SubQuery subQuery = query.getSubQuery(ebid);

  if(subQuery == null) {
    out.write("<script type='text/javascript'>alert('no sub-query'); history.back(0); </script>");
    return;
  }

  if(subQuery == null) {
%>
<script type="text/javascript">
  alert("No Execution Block for" + ebid);
  document.history.back();
</script>
<%
    return;
  }
  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  String url = "querytasks.jsp?queryId=" + queryId + "&ebid=" + ebid + "&sortOrder=" + nextSortOrder + "&sort=";
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/style.css"/>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Query Detail Info</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Worker: <a href='index.jsp'><%=tajoWorker.getWorkerContext().getWorkerName()%></a></h2>
  <hr/>
  <h3><%=ebid.toString()%>(<%=subQuery.getState()%>)</h3>
  <div>Started:<%=df.format(subQuery.getStartTime())%> ~ <%=subQuery.getFinishTime() == 0 ? "-" : df.format(subQuery.getFinishTime())%></div>
  <hr/>
  <table border="1" width="100%" class="border_table">
    <tr><th><a href='<%=url%>id'>Id</a></th><th>Status</th><th><a href='<%=url%>startTime'>Start Time</a></th><th><a href='<%=url%>runTime'>Running Time</a></th><th><a href='<%=url%>host'>Host</a></th></tr>
    <%
      QueryUnit[] queryUnits = subQuery.getQueryUnits();
      JSPUtil.sortQueryUnit(queryUnits, sort, sortOrder);
      for(QueryUnit eachQueryUnit: queryUnits) {
    %>
    <tr>
      <td><%=eachQueryUnit.getId()%></td>
      <td><%=eachQueryUnit.getState()%></td>
      <td><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : df.format(eachQueryUnit.getLaunchTime())%></td>
      <td><%=eachQueryUnit.getLaunchTime() == 0 ? "-" : eachQueryUnit.getRunningTime() + " ms"%></td>
      <td><%=eachQueryUnit.getSucceededHost() == null ? "-" : eachQueryUnit.getSucceededHost()%></td>
    </tr>
    <%
      }
    %>
  </table>
  <%
  %>
</div>
</body>
</html>