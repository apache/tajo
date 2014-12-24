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

<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.catalog.*" %>
<%@ page import="org.apache.hadoop.http.HtmlQuoting" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%
    TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
    CatalogService catalog = master.getCatalog();

    List<FunctionDesc> functions = new ArrayList<FunctionDesc>(catalog.getFunctions());
    JSPUtil.sortFunctionDesc(functions);
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
    <h3>Catalog</h3>
    <div>
        <div style='float:left; margin-right:10px'><a href='catalogview.jsp'>[Table]</a></div>
        <div style='float:left; margin-right:10px'><a href='functions.jsp'>[Function]</a></div>
        <div style='clear:both'></div>
    </div>
    <p/>
    <table border="1" class='border_table'>
        <tr><th width='5%'>Name</th><th width='20%'>Signature</th><th width="5%">Type</th><th width='40%'>Description</th><th>Example</th></tr>
<%
    for(FunctionDesc eachFunction: functions) {
        String fullDecription = eachFunction.getDescription();
        if(eachFunction.getDetail() != null && !eachFunction.getDetail().isEmpty()) {
            fullDecription += "\n" + eachFunction.getDetail();
        }
%>
        <tr>
            <td><%=eachFunction.getFunctionName()%></td>
            <td><%=eachFunction.getHelpSignature()%></td>
            <td><%=eachFunction.getFuncType()%></td>
            <td><%=HtmlQuoting.quoteHtmlChars(fullDecription).replace("\n", "<br/>")%></td>
            <td><%=HtmlQuoting.quoteHtmlChars(eachFunction.getExample()).replace("\n", "<br/>")%></td>
        </tr>
<%
    }
%>
    </table>
</div>
</body>
</html>
