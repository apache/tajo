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

<%@ page import="org.apache.tajo.catalog.CatalogService" %>
<%@ page import="org.apache.tajo.catalog.Column" %>
<%@ page import="org.apache.tajo.catalog.TableDesc" %>
<%@ page import="org.apache.tajo.catalog.partition.PartitionMethodDesc" %>
<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.util.FileUtil" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.util.Collection" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Map" %>
<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  CatalogService catalog = master.getCatalog();

  String catalogType = request.getParameter("type");
  if(catalogType != null && "function".equals(catalogType)) {
%>
<script type="text/javascript">
    document.location.href = 'functions.jsp';
</script>
    return;
<%
  }
  String selectedDatabase = request.getParameter("database");
  if(selectedDatabase == null || selectedDatabase.trim().isEmpty()) {
    selectedDatabase = "default";
  }

  TableDesc tableDesc = null;
  String selectedTable = request.getParameter("table");
  if(selectedTable != null && !selectedTable.trim().isEmpty()) {
    tableDesc = catalog.getTableDesc(selectedDatabase, selectedTable);
  } else {
    selectedTable = "";
  }

  //TODO filter with database
  Collection<String> tableNames = catalog.getAllTableNames(selectedDatabase);
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
  <table width="100%" border='0'>
    <tr>
      <!-- left -->
      <td width="20%" valign="top">
        <div>
          <b>Database:</b>
          <form action='catalogview.jsp' method='GET'>
            <select name="database" width="190" style="width: 190px" onchange="document.location.href='catalogview.jsp?database=' + this.value">
            <%
            for (String databaseName : catalog.getAllDatabaseNames()) {
              if (selectedDatabase.equals(databaseName)) { %>
                <option value="<%=databaseName%>" selected><%=databaseName%>
            <%} else {%>
                <option value="<%=databaseName%>"><%=databaseName%></option>
            <%}
            }
            %>
            </select>
            <input type="submit" value="Select"/>
          </form>
        </div>
        <!-- table list -->
        <div style='margin-top:5px'>
<%
  if(tableNames == null || tableNames.isEmpty()) {
    out.write("No tables");
  } else {
%>
          <table width="100%" border="1" class="border_table">
            <tr><th>Table Name</th></tr>
<%
    for(String eachTableName: tableNames) {
      String bold = "";
      if(eachTableName.equals(selectedTable)) {
        bold = "font-weight:bold";
      }
      String detailLink = "catalogview.jsp?database=" + selectedDatabase + "&table=" + eachTableName;
      out.write("<tr><td><span style='" + bold + "'><a href='" + detailLink + "'>" + eachTableName + "</a></span></td></tr>");
    }
%>
          </table>
<%
  }
%>
        </div>
      </td>
      <!-- right -->
      <td width="80%" valign="top">
        <div style='margin-left: 15px'>
          <div style='font-weight:bold'>Table name: <%=selectedTable%></div>
          <div style='margin-top:5px'>
<%
    if(tableDesc != null) {
      List<Column> columns = tableDesc.getSchema().getColumns();
      out.write("<table border='1' class='border_table'><tr><th>No</th><th>Column name</th><th>Type</th></tr>");
      int columnIndex = 1;
      for(Column eachColumn: columns) {
        out.write("<tr><td width='30' align='right'>" + columnIndex + "</td><td width='320'>" + eachColumn.getSimpleName() + "</td><td width='150'>" + eachColumn.getDataType().getType() + "</td></tr>");
        columnIndex++;
      }
      out.write("</table>");
      out.write("</div>");

      if (tableDesc.getPartitionMethod() != null) {
        PartitionMethodDesc partition = tableDesc.getPartitionMethod();
        List<Column> partitionColumns = partition.getExpressionSchema().getColumns();
        String partitionColumnStr = "";
        String prefix = "";
        for (Column eachColumn: partitionColumns) {
          partitionColumnStr += prefix + eachColumn.toString();
          prefix = "<br/>";
        }
        out.write("<div style='margin-top:10px'>");
        out.write("  <div style=''>Partition</div>");
        out.write("  <table border='1' class='border_table'>");
        out.write("    <tr><td width='100'>Type</td><td width='410'>" + partition.getPartitionType().name() + "</td></tr>");
        out.write("    <tr><td>Columns</td><td>" + partitionColumnStr + "</td></tr>");
        out.write("  </table>");
        out.write("</div>");
      }
      String optionStr = "";
      String prefix = "";
      for(Map.Entry<String, String> entry: tableDesc.getMeta().toMap().entrySet()) {
        optionStr += prefix + "'" + entry.getKey() + "'='" + entry.getValue() + "'";
        prefix = "<br/>";
      }
%>
          <div style='margin-top:10px'>
            <div style=''>Detail</div>
            <table border="1" class='border_table'>
              <tr><td width='100'>Table path</td><td width='410'><%=tableDesc.getPath()%></td></tr>
              <tr><td>Store type</td><td><%=tableDesc.getMeta().getStoreType()%></td></tr>
              <tr><td># rows</td><td><%=(tableDesc.hasStats() ? ("" + tableDesc.getStats().getNumRows()) : "-")%></td></tr>
              <tr><td>Volume</td><td><%=(tableDesc.hasStats() ? FileUtil.humanReadableByteCount(tableDesc.getStats().getNumBytes(),true) : "-")%></td></tr>
              <tr><td>Options</td><td><%=optionStr%></td></tr>
            </table>
          </div>
        </div>
<%
    }
%>
      </td>
    </tr>
  </table>
</div>
</body>
</html>
