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
<%@ page import="org.apache.tajo.QueryUnitId" %>
<%@ page import="org.apache.tajo.catalog.proto.CatalogProtos" %>
<%@ page import="org.apache.tajo.catalog.statistics.TableStats" %>
<%@ page import="org.apache.tajo.ipc.TajoWorkerProtocol" %>
<%@ page import="org.apache.tajo.master.querymaster.Query" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryMasterTask" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryUnit" %>
<%@ page import="org.apache.tajo.master.querymaster.SubQuery" %>
<%@ page import="org.apache.tajo.storage.DataLocation" %>
<%@ page import="org.apache.tajo.storage.fragment.FileFragment" %>
<%@ page import="org.apache.tajo.storage.fragment.FragmentConvertor" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.FetchImpl" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="java.net.URI" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.Set" %>

<%
    String paramQueryId = request.getParameter("queryId");
    String paramEbId = request.getParameter("ebid");
    String status = request.getParameter("status");
    if(status == null || status.isEmpty() || "null".equals(status)) {
        status = "ALL";
    }

    QueryId queryId = TajoIdUtils.parseQueryId(paramQueryId);
    ExecutionBlockId ebid = TajoIdUtils.createExecutionBlockId(paramEbId);

    int queryUnitSeq = Integer.parseInt(request.getParameter("queryUnitSeq"));
    TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
    QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
            .getQueryMasterManagerService().getQueryMaster().getQueryMasterTask(queryId, true);

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

    QueryUnitId queryUnitId = new QueryUnitId(ebid, queryUnitSeq);
    QueryUnit queryUnit = subQuery.getQueryUnit(queryUnitId);
    if(queryUnit == null) {
%>
<script type="text/javascript">
    alert("No QueryUnit for" + queryUnitId);
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
    for (CatalogProtos.FragmentProto eachFragment : queryUnit.getAllFragments()) {
        FileFragment fileFragment = FragmentConvertor.convert(FileFragment.class, eachFragment);
        fragmentInfo += delim + fileFragment.toString();
        delim = "<br/>";
    }

    String fetchInfo = "";
    delim = "";
    for (Map.Entry<String, Set<FetchImpl>> e : queryUnit.getFetchMap().entrySet()) {
        fetchInfo += delim + "<b>" + e.getKey() + "</b>";
        delim = "<br/>";
        for (FetchImpl f : e.getValue()) {
            for (URI uri : f.getSimpleURIs()){
                fetchInfo += delim + uri;
            }
        }
    }

    String dataLocationInfos = "";
    delim = "";
    for(DataLocation eachLocation: queryUnit.getDataLocations()) {
        dataLocationInfos += delim + eachLocation.toString();
        delim = "<br/>";
    }

    int numShuffles = queryUnit.getShuffleOutpuNum();
    String shuffleKey = "-";
    String shuffleFileName = "-";
    if(numShuffles > 0) {
        TajoWorkerProtocol.ShuffleFileOutput shuffleFileOutputs = queryUnit.getShuffleFileOutputs().get(0);
        shuffleKey = "" + shuffleFileOutputs.getPartId();
        shuffleFileName = shuffleFileOutputs.getFileName();
    }

    //int numIntermediateData = queryUnit.getIntermediateData() == null ? 0 : queryUnit.getIntermediateData().size();
    TableStats inputStat = queryUnit.getLastAttempt().getInputStats();
    TableStats outputStat = queryUnit.getLastAttempt().getResultStats();
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
        <tr><td width="200" align="right">ID</td><td><%=queryUnit.getId()%></td></tr>
        <tr><td align="right">Progress</td><td><%=JSPUtil.percentFormat(queryUnit.getLastAttempt().getProgress())%>%</td></tr>
        <tr><td align="right">State</td><td><%=queryUnit.getState()%></td></tr>
        <tr><td align="right">Launch Time</td><td><%=queryUnit.getLaunchTime() == 0 ? "-" : df.format(queryUnit.getLaunchTime())%></td></tr>
        <tr><td align="right">Finish Time</td><td><%=queryUnit.getFinishTime() == 0 ? "-" : df.format(queryUnit.getFinishTime())%></td></tr>
        <tr><td align="right">Running Time</td><td><%=queryUnit.getLaunchTime() == 0 ? "-" : queryUnit.getRunningTime() + " ms"%></td></tr>
        <tr><td align="right">Host</td><td><%=queryUnit.getSucceededHost() == null ? "-" : queryUnit.getSucceededHost()%></td></tr>
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