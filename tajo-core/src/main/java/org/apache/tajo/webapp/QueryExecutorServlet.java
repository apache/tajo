package org.apache.tajo.webapp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.JSPUtil;
import org.apache.tajo.util.TajoIdUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.exception.ReturnStateUtil.isError;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class QueryExecutorServlet extends HttpServlet {
  private static final Log LOG = LogFactory.getLog(QueryExecutorServlet.class);
  private static final long serialVersionUID = -1517586415463171579L;

  transient ObjectMapper om = new ObjectMapper();

  //queryRunnerId -> QueryRunner
  //TODO We must handle the session.
  private transient final Map<String, QueryRunner> queryRunners = new HashMap<>();

  private transient TajoConf tajoConf;
  private transient TajoClient tajoClient;

  private transient ExecutorService queryRunnerExecutor = Executors.newFixedThreadPool(5);

  private void writeObject(java.io.ObjectOutputStream stream) throws java.io.IOException {
    throw new NotSerializableException( getClass().getName() );
  }

  private void readObject(java.io.ObjectInputStream stream) throws java.io.IOException, ClassNotFoundException {
    throw new NotSerializableException( getClass().getName() );
  }

  @Override
  public void init(ServletConfig config) throws ServletException {
    om.getDeserializationConfig().disable(
        DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    try {
      tajoConf = new TajoConf();
      tajoClient = new TajoClientImpl(ServiceTrackerFactory.get(tajoConf));

      new QueryRunnerCleaner().start();
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void service(HttpServletRequest request,
                      HttpServletResponse response) throws ServletException, IOException {
    String action = request.getParameter("action");
    Map<String, Object> returnValue = new HashMap<>();
    try {
      if(tajoClient == null) {
        errorResponse(response, "TajoClient not initialized");
        return;
      }
      if(action == null || action.trim().isEmpty()) {
        errorResponse(response, "no action parameter.");
        return;
      }

      if("runQuery".equals(action)) {
        String prevQueryRunnerId = request.getParameter("prevQueryId");
        if (prevQueryRunnerId != null) {
          synchronized (queryRunners) {
            QueryRunner runner = queryRunners.remove(prevQueryRunnerId);
            if (runner != null) runner.setStop();
          }
        }

        float allowedMemoryRatio = 0.5f; // if TajoMaster memory usage is over 50%, the request will be canceled
        long maxMemory = Runtime.getRuntime().maxMemory();
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if(usedMemory > maxMemory * allowedMemoryRatio) {
          errorResponse(response, "Allowed memory size of " +
              (maxMemory * allowedMemoryRatio) / (1024 * 1024) + " MB exhausted");
          return;
        }

        String query = request.getParameter("query");
        if(query == null || query.trim().isEmpty()) {
          errorResponse(response, "No query parameter");
          return;
        }

        String queryRunnerId = null;
        while(true) {
          synchronized(queryRunners) {
            queryRunnerId = "" + System.currentTimeMillis();
            if(!queryRunners.containsKey(queryRunnerId)) {
              break;
            }
          }

          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
          }
        }
        String database = request.getParameter("database");
        QueryRunner queryRunner = new QueryRunner(queryRunnerId, query, database);
        try {
          queryRunner.sizeLimit = Integer.parseInt(request.getParameter("limitSize"));
        } catch (java.lang.NumberFormatException nfe) {
          queryRunner.sizeLimit = 1048576;
        }
        try {
          queryRunner.rowLimit = Integer.parseInt(request.getParameter("limitRow"));
        } catch (java.lang.NumberFormatException nfe) {
          queryRunner.rowLimit = 3000000;
        }
        synchronized(queryRunners) {
          queryRunners.put(queryRunnerId, queryRunner);
        }
        queryRunnerExecutor.submit(queryRunner);
        returnValue.put("queryRunnerId", queryRunnerId);
      } else if("getQueryProgress".equals(action)) {
        synchronized(queryRunners) {
          String queryRunnerId = request.getParameter("queryRunnerId");
          QueryRunner queryRunner = queryRunners.get(queryRunnerId);
          if(queryRunner == null) {
            errorResponse(response, "No query info:" + queryRunnerId);
            return;
          }
          if(queryRunner.error != null) {
            errorResponse(response, queryRunner.error);
            return;
          }
          SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

          returnValue.put("progress", queryRunner.progress);
          returnValue.put("startTime", df.format(queryRunner.startTime));
          returnValue.put("finishTime", queryRunner.finishTime == 0 ? "-" : df.format(queryRunner.startTime));
          returnValue.put("runningTime", JSPUtil.getElapsedTime(queryRunner.startTime, queryRunner.finishTime));
        }
      } else if("getQueryResult".equals(action)) {
        synchronized(queryRunners) {
          String queryRunnerId = request.getParameter("queryRunnerId");
          QueryRunner queryRunner = queryRunners.get(queryRunnerId);
          if(queryRunner == null) {
            errorResponse(response, "No query info:" + queryRunnerId);
            return;
          }
          if(queryRunner.error != null) {
            errorResponse(response, queryRunner.error);
            return;
          }
          returnValue.put("resultSize", queryRunner.resultRows);
          returnValue.put("resultData", queryRunner.queryResult);
          returnValue.put("resultColumns", queryRunner.columnNames);
          returnValue.put("runningTime", JSPUtil.getElapsedTime(queryRunner.startTime, queryRunner.finishTime));
        }
      } else if("clearAllQueryRunner".equals(action)) {
        synchronized(queryRunners) {
          for(QueryRunner eachQueryRunner: queryRunners.values()) {
            eachQueryRunner.setStop();
          }
          queryRunners.clear();
        }
      } else if("killQuery".equals(action)) {
        String queryId = request.getParameter("queryId");
        if(queryId == null || queryId.trim().isEmpty()) {
          errorResponse(response, "No queryId parameter");
          return;
        }
        QueryStatus status = tajoClient.killQuery(TajoIdUtils.parseQueryId(queryId));

        if (status.getState() == TajoProtos.QueryState.QUERY_KILLED) {
          returnValue.put("successMessage", queryId + " is killed successfully.");
        } else if (status.getState() == TajoProtos.QueryState.QUERY_KILL_WAIT) {
          returnValue.put("successMessage", queryId + " will be finished after a while.");
        } else {
          errorResponse(response, "ERROR:" + status.getErrorMessage());
          return;
        }
      }

      returnValue.put("success", "true");
      writeHttpResponse(response, returnValue);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      errorResponse(response, e);
    }
  }

  private void errorResponse(HttpServletResponse response, Exception e) throws IOException {
    errorResponse(response, e.getMessage() + "\n" + StringUtils.stringifyException(e));
  }

  private void errorResponse(HttpServletResponse response, String message) throws IOException {
    Map<String, Object> errorMessage = new HashMap<>();
    errorMessage.put("success", "false");
    errorMessage.put("errorMessage", message);
    writeHttpResponse(response, errorMessage);
  }

  private void writeHttpResponse(HttpServletResponse response, Map<String, Object> outputMessage) throws IOException {
    response.setContentType("text/html");

    OutputStream out = response.getOutputStream();
    out.write(om.writeValueAsBytes(outputMessage));

    out.flush();
    out.close();
  }

  class QueryRunnerCleaner extends Thread {
    public void run() {
      List<QueryRunner> queryRunnerList;
      synchronized(queryRunners) {
        queryRunnerList = new ArrayList<>(queryRunners.values());
        for(QueryRunner eachQueryRunner: queryRunnerList) {
          if(!eachQueryRunner.running.get() &&
              (System.currentTimeMillis() - eachQueryRunner.finishTime > 180 * 1000)) {
            queryRunners.remove(eachQueryRunner.queryRunnerId);
          }
        }
      }
    }
  }

  class QueryRunner extends Thread {
    long startTime;
    long finishTime;

    String queryRunnerId;

    ClientProtos.SubmitQueryResponse response;
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicBoolean stop = new AtomicBoolean(false);
    QueryId queryId;
    String query;
    String database;
    long resultRows;
    int sizeLimit;
    long rowLimit;
    Exception error;

    AtomicInteger progress = new AtomicInteger(0);

    List<String> columnNames = new ArrayList<>();

    List<List<Object>> queryResult;

    public QueryRunner(String queryRunnerId, String query) {
      this (queryRunnerId, query, "default");
    }

    public QueryRunner(String queryRunnerId, String query, String database) {
      this.queryRunnerId = queryRunnerId;
      this.query = query;
      this.database = database;
    }

    public void setStop() {
      this.stop.set(true);
      this.interrupt();
    }

    public void run() {

      startTime = System.currentTimeMillis();

      try {
        if (!tajoClient.getCurrentDatabase().equals(database)) {
          tajoClient.selectDatabase(database);
        }

        response = tajoClient.executeQuery(query);

        if (isError(response.getState())) {
          StringBuffer errorMessage = new StringBuffer(response.getState().getMessage());
          String modifiedMessage;

          if (errorMessage.length() > 200) {
            modifiedMessage = errorMessage.substring(0, 200);
          } else {
            modifiedMessage = errorMessage.toString();
          }

          String lineSeparator = System.getProperty("line.separator");
          modifiedMessage = modifiedMessage.replaceAll(lineSeparator, "<br/>");

          error = new Exception(modifiedMessage);

        } else {

          switch (response.getResultType()) {
          case ENCLOSED:
            getSimpleQueryResult(response);
            break;
          case FETCH:
            queryId = new QueryId(response.getQueryId());
            getQueryResult(queryId);
            break;
          default:;
          }

          progress.set(100);
        }

      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        error = e;
      } finally {
        running.set(false);

        finishTime = System.currentTimeMillis();

        if (queryId != null) {
          try {
            tajoClient.closeQuery(queryId);
          } catch (Throwable e) {
            LOG.warn(e);
          }
        }
      }
    }

    private void getSimpleQueryResult(ClientProtos.SubmitQueryResponse response) {
      ResultSet res = null;
      try {
        QueryId queryId = new QueryId(response.getQueryId());
        TableDesc desc = new TableDesc(response.getTableDesc());

        if (response.getMaxRowNum() < 0 && queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
          // non-forwarded INSERT INTO query does not have any query id.
          // In this case, it just returns succeeded query information without printing the query results.
        } else {
          res = TajoClientUtil.createResultSet(tajoClient, response, sizeLimit);
          MakeResultText(res, desc);
        }
        progress.set(100);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        error = e;
      } finally {
        if (res != null) {
          try {
            res.close();
          } catch (SQLException e) {
          }
        }
      }
    }

    private QueryStatus waitForComplete(QueryId queryid) throws TajoException {
      QueryStatus status = null;

      while (!stop.get()) {

        try {
          Thread.sleep(150);
        } catch(InterruptedException e) {
          break;
        }

        status = tajoClient.getQueryStatus(queryid);
        if (status.getState() == TajoProtos.QueryState.QUERY_MASTER_INIT
            || status.getState() == TajoProtos.QueryState.QUERY_MASTER_LAUNCHED) {
          continue;
        }

        if (status.getState() == TajoProtos.QueryState.QUERY_RUNNING
            || status.getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
          int progressValue = (int) (status.getProgress() * 100.0f);
          if(progressValue == 100)  {
            progressValue = 99;
          }
          progress.set(progressValue);
        }
        if (status.getState() != TajoProtos.QueryState.QUERY_RUNNING
            && status.getState() != TajoProtos.QueryState.QUERY_NOT_ASSIGNED) {
          break;
        }

        try {
          Thread.sleep(100);
        } catch(InterruptedException e) {
          break;
        }
      }

      return status;
    }

    private void getQueryResult(QueryId tajoQueryId) {
      // query execute
      try {
        QueryStatus status = waitForComplete(tajoQueryId);

        if(status == null) {
          LOG.error("Query Status is null");
          error = new Exception("Query Status is null");
          return;
        }
        if (status.getState() == TajoProtos.QueryState.QUERY_ERROR ||
            status.getState() == TajoProtos.QueryState.QUERY_FAILED) {
          error = new Exception(status.getErrorMessage());
        } else if (status.getState() == TajoProtos.QueryState.QUERY_KILLED) {
          LOG.info(queryId + " is killed.");
          error = new Exception(queryId + " is killed.");
        } else {
          if (status.getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
            if (status.hasResult()) {
              ResultSet res = null;
              try {
                ClientProtos.GetQueryResultResponse response = tajoClient.getResultResponse(tajoQueryId);
                TableDesc desc = CatalogUtil.newTableDesc(response.getTableDesc());
                tajoConf.setVar(TajoConf.ConfVars.USERNAME, response.getTajoUserName());
                res = new FetchResultSet(tajoClient, desc.getLogicalSchema(), queryId, sizeLimit);

                MakeResultText(res, desc);

              } finally {
                if (res != null) {
                  res.close();
                }
                progress.set(100);
              }
            } else { // CTAS or INSERT (OVERWRITE) INTO
              progress.set(100);
              try {
                tajoClient.closeQuery(queryId);
              } catch (Exception e) {
                LOG.warn(e, e);
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        error = e;
      }
    }

    private void MakeResultText(ResultSet res, TableDesc desc) throws SQLException {
      ResultSetMetaData rsmd = res.getMetaData();
      resultRows = desc.getStats() == null ? 0 : desc.getStats().getNumRows();
      if (resultRows <= 0) {
        resultRows = 1000;
      }
      LOG.info("Tajo Query Result: " + desc.getUri() + "\n");

      int numOfColumns = rsmd.getColumnCount();
      for(int i = 0; i < numOfColumns; i++) {
        columnNames.add(rsmd.getColumnName(i + 1));
      }
      queryResult = new ArrayList<>();

      int currentResultSize = 0;
      int rowCount = 0;
      while (res.next()) {
        if(rowCount > rowLimit || currentResultSize > sizeLimit) {
          break;
        }
        List<Object> row = new ArrayList<>();
        for(int i = 0; i < numOfColumns; i++) {
          String columnValue = String.valueOf(res.getObject(i + 1));
          try {
            currentResultSize += columnValue.getBytes(Bytes.UTF8_ENCODING).length;
          } catch (Exception e) {
          }
          row.add(columnValue);
        }
        queryResult.add(row);
        rowCount++;
      }
    }
  }
}
