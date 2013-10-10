<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.catalog.*" %>
<%@ page import="org.apache.tajo.util.FileUtil" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
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
  if(progressTimer != null) {
    alert("Already query running.");
    return;
  }
  init();
  var query = $("#query").val();

  $.ajax({
    type: "POST",
    url: "query_exec",
    data: { action: "runQuery", query: query}
  })
  .done(function(msg) {
    var resultJson = $.parseJSON(msg);
    if(resultJson.success == "false") {
      clearTimer();
      alert(resultJson.errorMessage);
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
          alert(resultJson.errorMessage);
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
    return "<div style='color:#ff0000; margin-top: 5px'>" + status.errorMessage + "</div>";
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
    var resultJson = $.parseJSON(msg);
    if(resultJson.success == "false") {
      alert(resultJson.errorMessage);
      $("#queryStatus").html(getQueryStatusHtml(resultJson));
      return;
    }
    console.log(resultJson);
    $("#queryResult").html("");
    var resultColumns = resultJson.resultColumns;
    var resultData = resultJson.resultData;

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
    }
    resultTable += "</table>";
    $("#queryResult").html(resultTable);
  });
}

</script>
</head>

<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Master: <%=master.getMasterName()%></h2>
  <hr/>
  <h3>Query</h3>
  <textarea id="query" style="width:800px; height:250px; font-family:Tahoma; font-size:12px;"></textarea>
  <p/>
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
</div>
</body>
</html>
