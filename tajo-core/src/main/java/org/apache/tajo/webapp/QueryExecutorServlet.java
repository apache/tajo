package org.apache.tajo.webapp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.TajoResultSet;
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
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

  ObjectMapper om = new ObjectMapper();

  //queryRunnerId -> QueryRunner
  private final Map<String, QueryRunner> queryRunners = new HashMap<String, QueryRunner>();

  private TajoClient tajoClient;

  private ExecutorService queryRunnerExecutor = Executors.newFixedThreadPool(5);

  private QueryRunnerCleaner queryRunnerCleaner;
  @Override
  public void init(ServletConfig config) throws ServletException {
    om.getDeserializationConfig().disable(
        DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    try {
      tajoClient = new TajoClient(new TajoConf());

      queryRunnerCleaner = new QueryRunnerCleaner();
      queryRunnerCleaner.start();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public void service(HttpServletRequest request,
                      HttpServletResponse response) throws ServletException, IOException {
    String action = request.getParameter("action");
    Map<String, Object> returnValue = new HashMap<String, Object>();
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
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
            }
          }
        }
        QueryRunner queryRunner = new QueryRunner(queryRunnerId, query);
        try {
          queryRunner.sizeLimit = Integer.parseInt(request.getParameter("limitSize"));
        } catch (java.lang.NumberFormatException nfe) {
          queryRunner.sizeLimit = 1048576;
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
          returnValue.put("numOfRows", queryRunner.numOfRows);
          returnValue.put("resultSize", queryRunner.resultSize);
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
    Map<String, Object> errorMessage = new HashMap<String, Object>();
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
        queryRunnerList = new ArrayList<QueryRunner>(queryRunners.values());
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

    ClientProtos.SubmitQueryResponse queryRespons;
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicBoolean stop = new AtomicBoolean(false);
    QueryId queryId;
    String query;
    long resultSize;
    int sizeLimit;
    long numOfRows;
    Exception error;

    AtomicInteger progress = new AtomicInteger(0);

    List<String> columnNames = new ArrayList<String>();

    List<List<Object>> queryResult;

    public QueryRunner(String queryRunnerId, String query) {
      this.queryRunnerId = queryRunnerId;
      this.query = query;
    }

    public void setStop() {
      this.stop.set(true);
      this.interrupt();
    }

    public void run() {
      startTime = System.currentTimeMillis();
      try {
        queryRespons = tajoClient.executeQuery(query);
        if (queryRespons.getResultCode() == ClientProtos.ResultCode.OK) {
          QueryId queryId = null;
          try {
            queryId = new QueryId(queryRespons.getQueryId());
            getQueryResult(queryId);
          } finally {
            if (queryId != null) {
              tajoClient.closeQuery(queryId);
            }
          }
        } else {
          LOG.error("queryRespons.getResultCode() not OK:" + queryRespons.getResultCode());
          error = new Exception("queryRespons.getResultCode() not OK:" + queryRespons.getResultCode());
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        error = e;
      } finally {
        running.set(false);
        finishTime = System.currentTimeMillis();
      }
    }

    private void getQueryResult(QueryId tajoQueryId) {
      // query execute
      try {
        QueryStatus status = null;

        while (!stop.get()) {
          try {
            Thread.sleep(1000);
          } catch(InterruptedException e) {
            break;
          }
          status = tajoClient.getQueryStatus(tajoQueryId);
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
        }

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
                tajoClient.getConf().setVar(TajoConf.ConfVars.USERNAME, response.getTajoUserName());
                res = new TajoResultSet(tajoClient, queryId, tajoClient.getConf(), desc);

                ResultSetMetaData rsmd = res.getMetaData();
                resultSize = desc.getStats().getNumBytes();
                LOG.info("Tajo Query Result: " + desc.getPath() + "\n");

                int numOfColumns = rsmd.getColumnCount();
                for(int i = 0; i < numOfColumns; i++) {
                  columnNames.add(rsmd.getColumnName(i + 1));
                }
                queryResult = new ArrayList<List<Object>>();

                if(sizeLimit < resultSize) {
                    numOfRows = (long)((float)(desc.getStats().getNumRows()) * ((float)sizeLimit / (float)resultSize));
                } else {
                    numOfRows = desc.getStats().getNumRows();
                }
                int rowCount = 0;
                boolean hasMoreData = false;
                while (res.next()) {
                  if(rowCount > numOfRows) {
                    hasMoreData = true;
                    break;
                  }
                  List<Object> row = new ArrayList<Object>();
                  for(int i = 0; i < numOfColumns; i++) {
                    row.add(res.getObject(i + 1).toString());
                  }
                  queryResult.add(row);
                  rowCount++;

                }
              } finally {
                if (res != null) {
                  res.close();
                }
                progress.set(100);
              }
            } else {
              error = new Exception(queryId + " no result");
            }
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        error = e;
      }
    }
  }
}
