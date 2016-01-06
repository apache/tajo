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
<%@ page import="org.apache.tajo.service.ServiceTracker" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.net.InetSocketAddress" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  String[] masterName = master.getMasterName().split(":");
  InetSocketAddress socketAddress = new InetSocketAddress(masterName[0], Integer.parseInt(masterName[1]));
  String masterLabel = socketAddress.getAddress().getHostName()+ ":" + socketAddress.getPort();

  ServiceTracker haService = master.getContext().getHAService();
  String activeLabel = "";
  if (haService != null) {
      if (haService.isActiveMaster()) {
      activeLabel = "<font color='#1e90ff'>(active)</font>";
    } else {
      activeLabel = "<font color='#1e90ff'>(backup)</font>";
    }
  }
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<link rel="stylesheet" type = "text/css" href = "/static/style.css" />
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Tajo</title>
<style type="text/css">
  #progress_bar {
    border:1px solid #000000;
    background:#ffffff;
    width:400px;
    height:16px;
    border-radius: 16px;
  }
  #progress_status {background:#fbcb46; width:0%; height:16px; border-radius: 10px; }
</style>
<script src="/static/js/jquery.js" type="text/javascript"></script>
<script type="text/javascript">
var progressInterval = 1000;
var progressTimer = null;
var queryRunnerId = null;
var PRINT_LIMIT = 25;
var SIZE_LIMIT = 104857600; // Limit size of displayed results.(Bytes)
var pageNum = 0;
var pageCount, storedColumns, storedData;

$(document).ready(function() {
  $('#btnSubmit').click(function() {
    runQuery();
  });
});

function init() {
  $("#progress_status").css("width", "0%");
  $("#progress_text").text("0%");
  $("#queryStatus").html("");
  $("#queryResult").html("");
  queryRunnerId = null;
}

function runQuery() {
  if(Math.ceil(Number($("#sizeLimit").val())) >= 2048) {
    SIZE_LIMIT = 2048 * 1024 * 1024 - 1;
  } else if(Math.ceil(Number($("#sizeLimit").val())) > 0) {
    SIZE_LIMIT = Number($("#sizeLimit").val()) * 1024 * 1024;
  }
  if(Math.ceil(Number($("#printLimit").val())) > 0) {
    PRINT_LIMIT = Number($("#printLimit").val());
  }
  if(progressTimer != null) {
    alert("Already query running.");
    return;
  }
  init();
  var query = $("#query").val();
  var sbox = document.getElementById("selectDatabase");

  $.ajax({
    type: "POST",
    url: "query_exec",
    data: { action: "runQuery", query: query, prevQueryId: queryRunnerId, limitSize:SIZE_LIMIT, database: sbox.options[sbox.selectedIndex].text }
  })
  .done(function(msg) {
    var resultJson = $.parseJSON(msg);
    if(resultJson.success == "false") {
      clearTimer();
      alert("query execution failed.");
      return;
    }
    queryRunnerId = resultJson.queryRunnerId;
    progressTimer = setInterval(function () {
      $.ajax({
        type: "GET",
        url: "query_exec",
        data: { action: "getQueryProgress", queryRunnerId: queryRunnerId }
      })
      .done(function( msg ) {
        var resultJson = $.parseJSON(msg);
        if(resultJson.success == "false") {
          clearTimer();
          alert("query execution failed.");
          $("#queryStatus").html(getQueryStatusHtml(resultJson));
          return;
        }
        var progress = parseInt(resultJson.progress, 0);
        $("#progress_status").css("width", progress + "%");
        $("#progress_text").text(progress + "%");
        $("#queryStatus").html(getQueryStatusHtml(resultJson));
        if(progress >= 100) {
          clearTimer();
          getResult();
        }
      });
    }, progressInterval);
  });
}

function clearTimer() {
  if(progressTimer != null) {
    clearInterval(progressTimer);
  }
  progressTimer = null;
}

function getQueryStatusHtml(status) {
  if(status.success == "false") {
    return "<div style='color:#ff0000; margin-top: 5px'><pre>" + status.errorMessage + "</pre></div>";
  } else {
    var statusHtml = "<div style='margin-top: 5px'>Start: " + status.startTime + "</div>";
    statusHtml += "<div style='margin-top: 5px'>Finish: " + status.finishTime + "</div>";
    statusHtml += "<div style='margin-top: 5px'> Running time: " + status.runningTime + "</div>";
    return statusHtml;
  }
}

function getResult() {
  $.ajax({
    type: "POST",
    url: "query_exec",
    data: { action: "getQueryResult", queryRunnerId: queryRunnerId }
  })
  .done(function(msg) {
    var printedLine = 0;
    var resultJson = $.parseJSON(msg);
    if(resultJson.success == "false") {
      alert(resultJson.errorMessage);
      $("#queryStatus").html(getQueryStatusHtml(resultJson));
      return;
    }
    $("#queryResult").html("");
    var resultColumns = resultJson.resultColumns;
    var resultData = resultJson.resultData;

    storedColumns = resultColumns;
    storedData = resultData;
    pageCount = Math.ceil((storedData.length / PRINT_LIMIT)) - 1 ;

    var resultTable = "<table width='100%' class='border_table'><tr>";
    for(var i = 0; i < resultColumns.length; i++) {
      resultTable += "<th>" + resultColumns[i] + "</th>";
    }
    resultTable += "</tr>";
    for(var i = 0; i < resultData.length; i++) {
      resultTable += "<tr>";
      for(var j = 0; j < resultData[i].length; j++) {
        resultTable += "<td>" + resultData[i][j] + "</td>";
      }
      resultTable += "</tr>";
      if(++printedLine >= PRINT_LIMIT) break;
    }
    resultTable += "</table>";
    $("#queryResult").html(resultTable);
    $("#queryResultTools").html("");
    $("#queryResultTools").append("<input type='button' value='Download to CSV' onclick='getCSV();'/> ");
    $("#queryResultTools").append("<input type='button' value='Prev' onclick='getPrev();'/> ");
    $("#queryResultTools").append("<input type='button' value='Next' onclick='getNext();'/> ");
    var selectPage = "<select id='selectPage'>";
    for(var i = 0; i <= pageCount; i++) {
      selectPage += "<option value="+i+">"+(i+1)+"</option>";
    }
    selectPage += "</select>";
    $("#queryResultTools").append(selectPage);
    $("#selectPage").change(getSelectedPage);
  })
}

function getCSV() {
  var csvData = "";
  var rowCount = storedData.length;
  var colCount = storedColumns.length;
  for(var colIndex = 0; colIndex < colCount; colIndex++) {
    if(colIndex == 0) {
      csvData += storedColumns[colIndex];
    } else {
      csvData += "," + storedColumns[colIndex];
    }
  }
  csvData += "\n";
  for(var rowIndex=0; rowIndex < rowCount; rowIndex++) {
    for(var colIndex = 0; colIndex < colCount; colIndex++){
      if(colIndex == 0) {
        csvData += storedData[rowIndex][colIndex];
      } else {
        csvData += "," + storedData[rowIndex][colIndex];
      }
    }
    csvData += "\n";
  }
  $("#csvData").val(csvData);
  $("#dataForm").submit();
}

function getNext() {
	var printedLine = 0;
	if(pageCount > pageNum) {
		pageNum++;
		document.getElementById("selectPage").options.selectedIndex = pageNum;
	}else {
		alert("There's no next page.");
		return;
	}
	getPage();
}

function getPrev() {
	if(pageNum > 0  ) {
		pageNum--;
		document.getElementById("selectPage").options.selectedIndex = pageNum;
	} else {
		alert("There's no previous page.");
		return;
	}
	getPage();
}

function getSelectedPage() {
  if(pageNum >= 0 &&  pageNum <= pageCount ) {
    pageNum = $("#selectPage option:selected").val();
  } else {
    alert("Out of range.");
    return;
  }
  getPage();
}

function getPage() {
  var printedLine = 0;
  $("#queryResult").html("");
  var resultTable = "<table width='100%' class='border_table'><tr>";
  for(var i = 0; i < storedColumns.length; i++) {
    resultTable += "<th>" + storedColumns[i] + "</th>";
  }
  resultTable += "</tr>";
  for(var i = pageNum * PRINT_LIMIT; i < storedData.length; i++) {
    resultTable += "<tr>";
    for(var j = 0; j < storedData[i].length; j++) {
      resultTable += "<td>" + storedData[i][j] + "</td>";
    }
    resultTable += "</tr>";
    if(++printedLine >= PRINT_LIMIT) break;
  }
  resultTable += "</table>";
  $("#queryResult").html(resultTable);
}

</script>
</head>

<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Master: <%=masterLabel%> <%=activeLabel%></h2>
  <hr/>
  <h3>Query</h3>
  Database :
  <select id="selectDatabase" name="database" width="190" style="width: 190px">
    <%
	for (String databaseName : master.getCatalog().getAllDatabaseNames()) {
	%>
	  <option value="<%=databaseName%>" <%= (databaseName.equals("default"))?"selected":"" %> ><%=databaseName%></option>
	<%
	}
	%>
  </select>
  <p />
<textarea id="query" style="width:800px; height:250px; font-family:Tahoma; font-size:12px;"></textarea>
  <p />
  Limit : <input id="sizeLimit" type="text" value="10" style="width:30px; text-align:center;" /> MB
  <p />
  Rows/Page : <input id="printLimit" type="text" value="25" style="width:30px; text-align:center;" />
  <hr />
  <input id="btnSubmit" type="submit" value="Submit">
  <hr/>
  <div>
    <div style="float:left; width:60px">Progress:</div>
    <div style='float:left; margin-left:10px'>
      <div id='progress_bar'>
        <div id='progress_status'></div>
      </div>
    </div>
    <div style='float:left; margin-left:10px;'>
      <div id="progress_text" style='font-family:Tahoma; font-size:14px; color:#000000; font-weight:bold'>0%</div>
    </div>
    <div style='clear:both'></div>
  </div>
  <div id="queryStatus">
  </div>
  <hr/>
  <h3>Query Result</h3>
  <div id="queryResult"></div>
  <hr/>
  <div id="queryResultTools"></div>
  <hr/>
  <div style="display:none;"><form name="dataForm" id="dataForm" method="post" action="getCSV.jsp"><input type="hidden" id="csvData" name="csvData" value="" /></form></div>
</div>
</body>
</html>
