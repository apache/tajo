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
<%@ page import="org.apache.tajo.master.QueryInProgress" %>
<%@ page import="org.apache.tajo.master.rm.Worker" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.StringUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.util.history.HistoryReader" %>
<%@ page import="org.apache.tajo.master.QueryInfo" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  List<QueryInProgress> runningQueries =
          new ArrayList<QueryInProgress>(master.getContext().getQueryJobManager().getSubmittedQueries());

  runningQueries.addAll(master.getContext().getQueryJobManager().getRunningQueries());
          JSPUtil.sortQueryInProgress(runningQueries, true);

  int currentPage = 1;
  if (request.getParameter("page") != null && !request.getParameter("page").isEmpty()) {
    currentPage = Integer.parseInt(request.getParameter("page"));
  }
  int pageSize = HistoryReader.DEFAULT_PAGE_SIZE;
  if (request.getParameter("pageSize") != null && !request.getParameter("pageSize").isEmpty()) {
    try {
      pageSize = Integer.parseInt(request.getParameter("pageSize"));
    } catch (NumberFormatException e) {
      pageSize = HistoryReader.DEFAULT_PAGE_SIZE;
    }
  }

  String keyword = request.getParameter("keyword");
  HistoryReader historyReader = master.getContext().getHistoryReader();
  List<QueryInfo> allFinishedQueries = historyReader.getQueries(keyword);

  int numOfFinishedQueries = allFinishedQueries.size();
  int totalPage = numOfFinishedQueries % pageSize == 0 ?
      numOfFinishedQueries / pageSize : numOfFinishedQueries / pageSize + 1;

  List<QueryInfo> finishedQueries = JSPUtil.getPageNavigationList(allFinishedQueries, currentPage, pageSize);

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  Map<Integer, Worker> workers = master.getContext().getResourceManager().getWorkers();
  Map<String, Integer> portMap = new HashMap<String, Integer>();

  Collection<Integer> queryMasters = master.getContext().getResourceManager().getQueryMasters();
  if (queryMasters == null || queryMasters.isEmpty()) {
    queryMasters = master.getContext().getResourceManager().getWorkers().keySet();
  }
  for(int eachQueryMasterKey: queryMasters) {
    Worker queryMaster = workers.get(eachQueryMasterKey);
    if(queryMaster != null) {
      portMap.put(queryMaster.getConnectionInfo().getHost(), queryMaster.getConnectionInfo().getHttpInfoPort());
    }
  }
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Tajo</title>
    <script src="/static/js/jquery.js" type="text/javascript"></script>
    <script type="text/javascript">

    function killQuery(queryId) {
        if (confirm("Are you sure to kill " + queryId + "?")) {
            $.ajax({
                type: "POST",
                url: "query_exec",
                data: { action: "killQuery", queryId: queryId }
            })
            .done(function(msg) {
                var resultJson = $.parseJSON(msg);
                if(resultJson.success == "false") {
                    alert(resultJson.errorMessage);
                } else {
                    alert(resultJson.successMessage);
                    location.reload();
                }
            })
        }
    }


  </script>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Master: <%=master.getMasterName()%> <%=JSPUtil.getMasterActiveLabel(master.getContext())%></h2>
  <hr/>
  <h3>Running Queries</h3>
<%
  if(runningQueries.isEmpty()) {
    out.write("No running queries");
  } else {
%>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>QueryId</th><th>Query Master</th><th>Started</th><th>Progress</th><th>Time</th><th>Status</th></th><th>sql</th><th>Kill Query</th></tr>
    <%
      for(QueryInProgress eachQuery: runningQueries) {
        long time = System.currentTimeMillis() - eachQuery.getQueryInfo().getStartTime();
        String detailView = "http://" + eachQuery.getQueryInfo().getQueryMasterHost() + ":" + portMap.get(eachQuery.getQueryInfo().getQueryMasterHost()) +
                "/querydetail.jsp?queryId=" + eachQuery.getQueryId() + "&startTime=" + eachQuery.getQueryInfo().getStartTime();
    %>
    <tr>
      <td><a href='<%=detailView%>'><%=eachQuery.getQueryId()%></a></td>
      <td><%=eachQuery.getQueryInfo().getQueryMasterHost()%></td>
      <td><%=df.format(eachQuery.getQueryInfo().getStartTime())%></td>
      <td><%=(int)(eachQuery.getQueryInfo().getProgress() * 100.0f)%>%</td>
      <td><%=StringUtils.formatTime(time)%></td>
      <td><%=eachQuery.getQueryInfo().getQueryState()%></td>
      <td><%=eachQuery.getQueryInfo().getSql()%></td>
      <td><input id="btnSubmit" type="submit" value="Kill" onClick="javascript:killQuery('<%=eachQuery.getQueryId()%>');"></td>
    </tr>
    <%
      }
    %>
  </table>
<%
  }
%>
  <p/>
  <hr/>
  <h3>Finished Queries</h3>
  <%
    if(finishedQueries.isEmpty()) {
      out.write("No finished queries");
    } else {
  %>
  <div align="right">
    <form action='query.jsp' method='GET'>
      Page Size: <input type="text" name="pageSize" value="<%=pageSize%>" size="5"/>
      &nbsp;<input type="submit" value="Submit">
    </form>
  </div>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>QueryId</th><th>Query Master</th><th>Started</th><th>Finished</th><th>Time</th><th>Status</th><th>sql</th></tr>
    <%
      for(QueryInfo eachQuery: finishedQueries) {
        long runTime = eachQuery.getFinishTime() > 0 ?
                eachQuery.getFinishTime() - eachQuery.getStartTime() : -1;
        String detailView = "querydetail.jsp?queryId=" + eachQuery.getQueryIdStr() + "&startTime=" + eachQuery.getStartTime();
    %>
    <tr>
      <td><a href='<%=detailView%>'><%=eachQuery.getQueryIdStr()%></a></td>
      <td><%=eachQuery.getQueryMasterHost()%></td>
      <td><%=df.format(eachQuery.getStartTime())%></td>
      <td><%=eachQuery.getFinishTime() > 0 ? df.format(eachQuery.getFinishTime()) : "-"%></td>
      <td><%=runTime == -1 ? "-" : StringUtils.formatTime(runTime) %></td>
      <td><%=eachQuery.getQueryState()%></td>
      <td><%=eachQuery.getSql()%></td>
    </tr>
    <%
      }
    %>
  </table>
  <div align="center">
    <%=JSPUtil.getPageNavigation(currentPage, totalPage, "query.jsp?pageSize=" + pageSize)%>
  </div>
  <p/>
<%
  }
%>
</div>
</body>
</html>
