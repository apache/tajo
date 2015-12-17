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

<%@ page import="org.apache.tajo.ExecutionBlockId" %>
<%@ page import="org.apache.tajo.QueryId" %>
<%@ page import="org.apache.tajo.ResourceProtos.FetchProto" %>
<%@ page import="org.apache.tajo.ResourceProtos.ShuffleFileOutput" %>
<%@ page import="org.apache.tajo.TaskId" %>
<%@ page import="org.apache.tajo.catalog.proto.CatalogProtos" %>
<%@ page import="org.apache.tajo.catalog.statistics.TableStats" %>
<%@ page import="org.apache.tajo.querymaster.*" %>
<%@ page import="org.apache.tajo.storage.DataLocation" %>
<%@ page import="org.apache.tajo.storage.fragment.Fragment" %>
<%@ page import="org.apache.tajo.storage.fragment.FragmentConvertor" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="java.net.URI" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.Set" %>
<%@ page import="org.apache.tajo.conf.TajoConf.ConfVars" %>

<%
    String paramQueryId = request.getParameter("queryId");
    String paramEbId = request.getParameter("ebid");
    String status = request.getParameter("status");
    if(status == null || status.isEmpty() || "null".equals(status)) {
        status = "ALL";
    }

    QueryId queryId = TajoIdUtils.parseQueryId(paramQueryId);
    ExecutionBlockId ebid = TajoIdUtils.createExecutionBlockId(paramEbId);

    int taskSeq = Integer.parseInt(request.getParameter("taskSeq"));
    TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
    QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
            .getQueryMasterManagerService().getQueryMaster().getQueryMasterTask(queryId);

    if(queryMasterTask == null) {
        String tajoMasterHttp = request.getScheme() + "://" + JSPUtil.getTajoMasterHttpAddr(tajoWorker.getConfig());
        response.sendRedirect(tajoMasterHttp + request.getRequestURI() + "?" + request.getQueryString());
        return;
    }

    int maxUrlLength = tajoWorker.getConfig().getInt(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH.name(),
            ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH.defaultIntVal);

    Query query = queryMasterTask.getQuery();
    Stage stage = query.getStage(ebid);

    if(stage == null) {
        out.write("<script type='text/javascript'>alert('no sub-query'); history.back(0); </script>");
        return;
    }

    if(stage == null) {
%>
<script type="text/javascript">
    alert("No Execution Block for" + ebid);
    document.history.back();
</script>
<%
        return;
    }
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    TaskId taskId = new TaskId(ebid, taskSeq);
    Task task = stage.getTask(taskId);
    if(task == null) {
%>
<script type="text/javascript">
    alert("No Task for" + taskId);
    document.history.back();
</script>
<%
        return;
    }

    String sort = request.getParameter("sort");
    String sortOrder = request.getParameter("sortOrder");

    String backUrl = "querytasks.jsp?queryId=" + paramQueryId + "&ebid=" + paramEbId + "&sort=" + sort + "&sortOrder=" + sortOrder + "&status=" + status;

    String fragmentInfo = "";
    String delim = "";
    for (CatalogProtos.FragmentProto eachFragment : task.getAllFragments()) {
        Fragment fragment = FragmentConvertor.convert(tajoWorker.getConfig(), eachFragment);
        fragmentInfo += delim + fragment.toString();
        delim = "<br/>";
    }

    String fetchInfo = "";
    delim = "";
    for (Map.Entry<String, Set<FetchProto>> e : task.getFetchMap().entrySet()) {
        fetchInfo += delim + "<b>" + e.getKey() + "</b>";
        delim = "<br/>";
        for (FetchProto f : e.getValue()) {
            for (URI uri : Repartitioner.createSimpleURIs(maxUrlLength, f)){
                fetchInfo += delim + uri;
            }
        }
    }

    String dataLocationInfos = "";
    delim = "";
    for(DataLocation eachLocation: task.getDataLocations()) {
        dataLocationInfos += delim + eachLocation.toString();
        delim = "<br/>";
    }

    int numShuffles = task.getShuffleOutpuNum();
    String shuffleKey = "-";
    String shuffleFileName = "-";
    if(numShuffles > 0) {
        ShuffleFileOutput shuffleFileOutputs = task.getShuffleFileOutputs().get(0);
        shuffleKey = "" + shuffleFileOutputs.getPartId();
        shuffleFileName = shuffleFileOutputs.getFileName();
    }

    TableStats inputStat = task.getLastAttempt().getInputStats();
    TableStats outputStat = task.getLastAttempt().getResultStats();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <link rel="stylesheet" type="text/css" href="/static/style.css"/>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Query Unit Detail</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
    <h2>Tajo Worker: <a href='index.jsp'><%=tajoWorker.getWorkerContext().getWorkerName()%></a></h2>
    <hr/>
    <h3><a href='<%=backUrl%>'><%=ebid.toString()%></a></h3>
    <hr/>
    <table border="1" width="100%" class="border_table">
        <tr><td width="200" align="right">ID</td><td><%=task.getId()%></td></tr>
        <tr><td align="right">Progress</td><td><%=JSPUtil.percentFormat(task.getLastAttempt().getProgress())%>%</td></tr>
        <tr><td align="right">State</td><td><%=task.getState()%></td></tr>
        <tr><td align="right">Launch Time</td><td><%=task.getLaunchTime() == 0 ? "-" : df.format(task.getLaunchTime())%></td></tr>
        <tr><td align="right">Finish Time</td><td><%=task.getFinishTime() == 0 ? "-" : df.format(task.getFinishTime())%></td></tr>
        <tr><td align="right">Running Time</td><td><%=task.getLaunchTime() == 0 ? "-" : task.getRunningTime() + " ms"%></td></tr>
        <tr><td align="right">Host</td><td><%=task.getSucceededWorker() == null ? "-" : task.getSucceededWorker().getHost()%></td></tr>
        <tr><td align="right">Shuffles</td><td># Shuffle Outputs: <%=numShuffles%>, Shuffle Key: <%=shuffleKey%>, Shuffle file: <%=shuffleFileName%></td></tr>
        <tr><td align="right">Data Locations</td><td><%=dataLocationInfos%></td></tr>
        <tr><td align="right">Fragment</td><td><%=fragmentInfo%></td></tr>
        <tr><td align="right">Input Statistics</td><td><%=JSPUtil.tableStatToString(inputStat)%></td></tr>
        <tr><td align="right">Output Statistics</td><td><%=JSPUtil.tableStatToString(outputStat)%></td></tr>
        <tr><td align="right">Fetches</td><td><%=fetchInfo%></td></tr>
    </table>
</div>
</body>
</html>