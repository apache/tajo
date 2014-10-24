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

package org.apache.tajo.master.querymaster;

import com.google.common.collect.Maps;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.plan.logical.CreateTableNode;
import org.apache.tajo.plan.logical.InsertNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.event.*;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Query implements EventHandler<QueryEvent> {
  private static final Log LOG = LogFactory.getLog(Query.class);

  // Facilities for Query
  private final TajoConf systemConf;
  private final Clock clock;
  private String queryStr;
  private Map<ExecutionBlockId, SubQuery> subqueries;
  private final EventHandler eventHandler;
  private final MasterPlan plan;
  private final StorageManager sm;
  QueryMasterTask.QueryMasterTaskContext context;
  private ExecutionBlockCursor cursor;

  // Query Status
  private final QueryId id;
  private long appSubmitTime;
  private long startTime;
  private long finishTime;
  private TableDesc resultDesc;
  private int completedSubQueryCount = 0;
  private int successedSubQueryCount = 0;
  private int killedSubQueryCount = 0;
  private int failedSubQueryCount = 0;
  private int erroredSubQueryCount = 0;
  private final List<String> diagnostics = new ArrayList<String>();

  // Internal Variables
  private final Lock readLock;
  private final Lock writeLock;
  private int priority = 100;

  // State Machine
  private final StateMachine<QueryState, QueryEventType, QueryEvent> stateMachine;
  private QueryState queryState;

  // Transition Handler
  private static final SingleArcTransition INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final DiagnosticsUpdateTransition DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final SubQueryCompletedTransition SUBQUERY_COMPLETED_TRANSITION = new SubQueryCompletedTransition();
  private static final QueryCompletedTransition QUERY_COMPLETED_TRANSITION = new QueryCompletedTransition();

  protected static final StateMachineFactory
      <Query,QueryState,QueryEventType,QueryEvent> stateMachineFactory =
      new StateMachineFactory<Query, QueryState, QueryEventType, QueryEvent>
          (QueryState.QUERY_NEW)

          // Transitions from NEW state
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_RUNNING,
              QueryEventType.START,
              new StartTransition())
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_NEW,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_KILLED,
              QueryEventType.KILL,
              new KillNewQueryTransition())
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_RUNNING,
              QueryEventType.SUBQUERY_COMPLETED,
              SUBQUERY_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_RUNNING,
              EnumSet.of(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_FAILED, QueryState.QUERY_KILLED,
                  QueryState.QUERY_ERROR),
              QueryEventType.QUERY_COMPLETED,
              QUERY_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_RUNNING,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_KILL_WAIT,
              QueryEventType.KILL,
              new KillSubQueriesTransition())
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from QUERY_SUCCEEDED state
          .addTransition(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_SUCCEEDED,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          // ignore-able transitions
          .addTransition(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_SUCCEEDED,
              QueryEventType.SUBQUERY_COMPLETED,
              SUBQUERY_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_SUCCEEDED,
              QueryEventType.KILL)
          .addTransition(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from KILL_WAIT state
          .addTransition(QueryState.QUERY_KILL_WAIT, QueryState.QUERY_KILL_WAIT,
              QueryEventType.SUBQUERY_COMPLETED,
              SUBQUERY_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_KILL_WAIT,
              EnumSet.of(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_FAILED, QueryState.QUERY_KILLED,
                  QueryState.QUERY_ERROR),
              QueryEventType.QUERY_COMPLETED,
              QUERY_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_KILL_WAIT, QueryState.QUERY_KILL_WAIT,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_KILL_WAIT, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able transitions
          .addTransition(QueryState.QUERY_KILL_WAIT, EnumSet.of(QueryState.QUERY_KILLED),
              QueryEventType.KILL,
              QUERY_COMPLETED_TRANSITION)

          // Transitions from FAILED state
          .addTransition(QueryState.QUERY_FAILED, QueryState.QUERY_FAILED,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_FAILED, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able transitions
          .addTransition(QueryState.QUERY_FAILED, QueryState.QUERY_FAILED,
              QueryEventType.KILL)

          // Transitions from ERROR state
          .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able transitions
          .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
              EnumSet.of(QueryEventType.KILL, QueryEventType.SUBQUERY_COMPLETED))

          .installTopology();

  public Query(final QueryMasterTask.QueryMasterTaskContext context, final QueryId id,
               final long appSubmitTime,
               final String queryStr,
               final EventHandler eventHandler,
               final MasterPlan plan) {
    this.context = context;
    this.systemConf = context.getConf();
    this.id = id;
    this.clock = context.getClock();
    this.appSubmitTime = appSubmitTime;
    this.queryStr = queryStr;
    subqueries = Maps.newHashMap();
    this.eventHandler = eventHandler;
    this.plan = plan;
    this.sm = context.getStorageManager();
    this.cursor = new ExecutionBlockCursor(plan, true);

    StringBuilder sb = new StringBuilder("\n=======================================================");
    sb.append("\nThe order of execution: \n");
    int order = 1;
    while (cursor.hasNext()) {
      ExecutionBlock currentEB = cursor.nextBlock();
      sb.append("\n").append(order).append(": ").append(currentEB.getId());
      order++;
    }
    sb.append("\n=======================================================");
    LOG.info(sb);
    cursor.reset();

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
    queryState = stateMachine.getCurrentState();
  }

  public float getProgress() {
    QueryState state = getState();
    if (state == QueryState.QUERY_SUCCEEDED) {
      return 1.0f;
    } else {
      int idx = 0;
      List<SubQuery> tempSubQueries = new ArrayList<SubQuery>();
      synchronized(subqueries) {
        tempSubQueries.addAll(subqueries.values());
      }

      float [] subProgresses = new float[tempSubQueries.size()];
      for (SubQuery subquery: tempSubQueries) {
        if (subquery.getState() != SubQueryState.NEW) {
          subProgresses[idx] = subquery.getProgress();
        } else {
          subProgresses[idx] = 0.0f;
        }
        idx++;
      }

      float totalProgress = 0.0f;
      float proportion = 1.0f / (float)(getExecutionBlockCursor().size() - 1); // minus one is due to

      for (int i = 0; i < subProgresses.length; i++) {
        totalProgress += subProgresses[i] * proportion;
      }

      return totalProgress;
    }
  }

  public long getAppSubmitTime() {
    return this.appSubmitTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime() {
    startTime = clock.getTime();
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime() {
    finishTime = clock.getTime();
  }

  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  protected void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }

  public TableDesc getResultDesc() {
    return resultDesc;
  }

  public void setResultDesc(TableDesc desc) {
    resultDesc = desc;
  }

  public MasterPlan getPlan() {
    return plan;
  }

  public StateMachine<QueryState, QueryEventType, QueryEvent> getStateMachine() {
    return stateMachine;
  }
  
  public void addSubQuery(SubQuery subquery) {
    subqueries.put(subquery.getId(), subquery);
  }
  
  public QueryId getId() {
    return this.id;
  }

  public SubQuery getSubQuery(ExecutionBlockId id) {
    return this.subqueries.get(id);
  }

  public Collection<SubQuery> getSubQueries() {
    return this.subqueries.values();
  }

  public QueryState getSynchronizedState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /* non-blocking call for client API */
  public QueryState getState() {
    return queryState;
  }

  public ExecutionBlockCursor getExecutionBlockCursor() {
    return cursor;
  }

  public static class StartTransition
      implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent queryEvent) {

      query.setStartTime();
      SubQuery subQuery = new SubQuery(query.context, query.getPlan(),
          query.getExecutionBlockCursor().nextBlock(), query.sm);
      subQuery.setPriority(query.priority--);
      query.addSubQuery(subQuery);

      subQuery.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_INIT));
      LOG.debug("Schedule unit plan: \n" + subQuery.getBlock().getPlan());
    }
  }

  public static class QueryCompletedTransition implements MultipleArcTransition<Query, QueryEvent, QueryState> {

    @Override
    public QueryState transition(Query query, QueryEvent queryEvent) {
      QueryCompletedEvent subQueryEvent = (QueryCompletedEvent) queryEvent;
      QueryState finalState;
      if (subQueryEvent.getState() == SubQueryState.SUCCEEDED) {
        finalizeQuery(query, subQueryEvent);
        finalState = QueryState.QUERY_SUCCEEDED;
      } else if (subQueryEvent.getState() == SubQueryState.FAILED) {
        finalState = QueryState.QUERY_FAILED;
      } else if (subQueryEvent.getState() == SubQueryState.KILLED) {
        finalState = QueryState.QUERY_KILLED;
      } else {
        finalState = QueryState.QUERY_ERROR;
      }
      query.eventHandler.handle(new QueryMasterQueryCompletedEvent(query.getId()));
      query.setFinishTime();
      return finalState;
    }

    private void finalizeQuery(Query query, QueryCompletedEvent event) {
      MasterPlan masterPlan = query.getPlan();

      ExecutionBlock terminal = query.getPlan().getTerminalBlock();
      DataChannel finalChannel = masterPlan.getChannel(event.getExecutionBlockId(), terminal.getId());
      Path finalOutputDir = commitOutputData(query);

      QueryHookExecutor hookExecutor = new QueryHookExecutor(query.context.getQueryMasterContext());
      try {
        hookExecutor.execute(query.context.getQueryContext(), query, event.getExecutionBlockId(),
            finalOutputDir);
      } catch (Exception e) {
        query.eventHandler.handle(new QueryDiagnosticsUpdateEvent(query.id, ExceptionUtils.getStackTrace(e)));
      }
    }

    /**
     * It moves a result data stored in a staging output dir into a final output dir.
     */
    public Path commitOutputData(Query query) {
      QueryContext queryContext = query.context.getQueryContext();
      Path stagingResultDir = new Path(queryContext.getStagingDir(), TajoConstants.RESULT_DIR_NAME);
      Path finalOutputDir;
      if (queryContext.hasOutputPath()) {
        finalOutputDir = queryContext.getOutputPath();
        try {
          FileSystem fs = stagingResultDir.getFileSystem(query.systemConf);

          if (queryContext.isOutputOverwrite()) { // INSERT OVERWRITE INTO

            // It moves the original table into the temporary location.
            // Then it moves the new result table into the original table location.
            // Upon failed, it recovers the original table if possible.
            boolean movedToOldTable = false;
            boolean committed = false;
            Path oldTableDir = new Path(queryContext.getStagingDir(), TajoConstants.INSERT_OVERWIRTE_OLD_TABLE_NAME);

            if (queryContext.hasPartition()) {
              // This is a map for existing non-leaf directory to rename. A key is current directory and a value is
              // renaming directory.
              Map<Path, Path> renameDirs = TUtil.newHashMap();
              // This is a map for recovering existing partition directory. A key is current directory and a value is
              // temporary directory to back up.
              Map<Path, Path> recoveryDirs = TUtil.newHashMap();

              try {
                if (!fs.exists(finalOutputDir)) {
                  fs.mkdirs(finalOutputDir);
                }

                visitPartitionedDirectory(fs, stagingResultDir, finalOutputDir, stagingResultDir.toString(),
                    renameDirs, oldTableDir);

                // Rename target partition directories
                for(Map.Entry<Path, Path> entry : renameDirs.entrySet()) {
                  // Backup existing data files for recovering
                  if (fs.exists(entry.getValue())) {
                    String recoveryPathString = entry.getValue().toString().replaceAll(finalOutputDir.toString(),
                        oldTableDir.toString());
                    Path recoveryPath = new Path(recoveryPathString);
                    fs.rename(entry.getValue(), recoveryPath);
                    fs.exists(recoveryPath);
                    recoveryDirs.put(entry.getValue(), recoveryPath);
                  }
                  // Delete existing directory
                  fs.delete(entry.getValue(), true);
                  // Rename staging directory to final output directory
                  fs.rename(entry.getKey(), entry.getValue());
                }

              } catch (IOException ioe) {
                // Remove created dirs
                for(Map.Entry<Path, Path> entry : renameDirs.entrySet()) {
                  fs.delete(entry.getValue(), true);
                }

                // Recovery renamed dirs
                for(Map.Entry<Path, Path> entry : recoveryDirs.entrySet()) {
                  fs.delete(entry.getValue(), true);
                  fs.rename(entry.getValue(), entry.getKey());
                }
                throw new IOException(ioe.getMessage());
              }
            } else {
              try {
                if (fs.exists(finalOutputDir)) {
                  fs.rename(finalOutputDir, oldTableDir);
                  movedToOldTable = fs.exists(oldTableDir);
                } else { // if the parent does not exist, make its parent directory.
                  fs.mkdirs(finalOutputDir.getParent());
                }

                fs.rename(stagingResultDir, finalOutputDir);
                committed = fs.exists(finalOutputDir);
              } catch (IOException ioe) {
                // recover the old table
                if (movedToOldTable && !committed) {
                  fs.rename(oldTableDir, finalOutputDir);
                }
              }
            }
          } else {
            NodeType queryType = queryContext.getCommandType();

            if (queryType == NodeType.INSERT) { // INSERT INTO an existing table

              NumberFormat fmt = NumberFormat.getInstance();
              fmt.setGroupingUsed(false);
              fmt.setMinimumIntegerDigits(3);

              if (queryContext.hasPartition()) {
                for(FileStatus eachFile: fs.listStatus(stagingResultDir)) {
                  if (eachFile.isFile()) {
                    LOG.warn("Partition table can't have file in a staging dir: " + eachFile.getPath());
                    continue;
                  }
                  moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputDir, fmt, -1);
                }
              } else {
                int maxSeq = StorageUtil.getMaxFileSequence(fs, finalOutputDir, false) + 1;
                for(FileStatus eachFile: fs.listStatus(stagingResultDir)) {
                  moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputDir, fmt, maxSeq++);
                }
              }
              // checking all file moved and remove empty dir
              verifyAllFileMoved(fs, stagingResultDir);
              FileStatus[] files = fs.listStatus(stagingResultDir);
              if (files != null && files.length != 0) {
                for (FileStatus eachFile: files) {
                  LOG.error("There are some unmoved files in staging dir:" + eachFile.getPath());
                }
              }
            } else { // CREATE TABLE AS SELECT (CTAS)
              fs.rename(stagingResultDir, finalOutputDir);
              LOG.info("Moved from the staging dir to the output directory '" + finalOutputDir);
            }
          }
        } catch (IOException e) {
          // TODO report to client
          e.printStackTrace();
        }
      } else {
        finalOutputDir = new Path(queryContext.getStagingDir(), TajoConstants.RESULT_DIR_NAME);
      }

      return finalOutputDir;
    }

    /**
     * This method sets a rename map which includes renamed staging directory to final output directory recursively.
     * If there exists some data files, this delete it for duplicate data.
     *
     *
     * @param fs
     * @param stagingPath
     * @param outputPath
     * @param stagingParentPathString
     * @throws IOException
     */
    private void visitPartitionedDirectory(FileSystem fs, Path stagingPath, Path outputPath,
                                        String stagingParentPathString,
                                        Map<Path, Path> renameDirs, Path oldTableDir) throws IOException {
      FileStatus[] files = fs.listStatus(stagingPath);

      for(FileStatus eachFile : files) {
        if (eachFile.isDirectory()) {
          Path oldPath = eachFile.getPath();

          // Make recover directory.
          String recoverPathString = oldPath.toString().replaceAll(stagingParentPathString,
          oldTableDir.toString());
          Path recoveryPath = new Path(recoverPathString);
          if (!fs.exists(recoveryPath)) {
            fs.mkdirs(recoveryPath);
          }

          visitPartitionedDirectory(fs, eachFile.getPath(), outputPath, stagingParentPathString,
          renameDirs, oldTableDir);
          // Find last order partition for renaming
          String newPathString = oldPath.toString().replaceAll(stagingParentPathString,
          outputPath.toString());
          Path newPath = new Path(newPathString);
          if (!isLeafDirectory(fs, eachFile.getPath())) {
           renameDirs.put(eachFile.getPath(), newPath);
          } else {
            if (!fs.exists(newPath)) {
             fs.mkdirs(newPath);
            }
          }
        }
      }
    }

    private boolean isLeafDirectory(FileSystem fs, Path path) throws IOException {
      boolean retValue = false;

      FileStatus[] files = fs.listStatus(path);
      for (FileStatus file : files) {
        if (fs.isDirectory(file.getPath())) {
          retValue = true;
          break;
        }
      }

      return retValue;
    }

    private boolean verifyAllFileMoved(FileSystem fs, Path stagingPath) throws IOException {
      FileStatus[] files = fs.listStatus(stagingPath);
      if (files != null && files.length != 0) {
        for (FileStatus eachFile: files) {
          if (eachFile.isFile()) {
            LOG.error("There are some unmoved files in staging dir:" + eachFile.getPath());
            return false;
          } else {
            if (verifyAllFileMoved(fs, eachFile.getPath())) {
              fs.delete(eachFile.getPath(), false);
            } else {
              return false;
            }
          }
        }
      }

      return true;
    }

    /**
     * Attach the sequence number to a path.
     *
     * @param path Path
     * @param seq sequence number
     * @param nf Number format
     * @return New path attached with sequence number
     * @throws IOException
     */
    private String replaceFileNameSeq(Path path, int seq, NumberFormat nf) throws IOException {
      String[] tokens = path.getName().split("-");
      if (tokens.length != 4) {
        throw new IOException("Wrong result file name:" + path);
      }
      return tokens[0] + "-" + tokens[1] + "-" + tokens[2] + "-" + nf.format(seq);
    }

    /**
     * Attach the sequence number to the output file name and than move the file into the final result path.
     *
     * @param fs FileSystem
     * @param stagingResultDir The staging result dir
     * @param fileStatus The file status
     * @param finalOutputPath Final output path
     * @param nf Number format
     * @param fileSeq The sequence number
     * @throws IOException
     */
    private void moveResultFromStageToFinal(FileSystem fs, Path stagingResultDir,
                                            FileStatus fileStatus, Path finalOutputPath,
                                            NumberFormat nf,
                                            int fileSeq) throws IOException {
      if (fileStatus.isDirectory()) {
        String subPath = extractSubPath(stagingResultDir, fileStatus.getPath());
        if (subPath != null) {
          Path finalSubPath = new Path(finalOutputPath, subPath);
          if (!fs.exists(finalSubPath)) {
            fs.mkdirs(finalSubPath);
          }
          int maxSeq = StorageUtil.getMaxFileSequence(fs, finalSubPath, false);
          for (FileStatus eachFile : fs.listStatus(fileStatus.getPath())) {
            moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputPath, nf, ++maxSeq);
          }
        } else {
          throw new IOException("Wrong staging dir:" + stagingResultDir + "," + fileStatus.getPath());
        }
      } else {
        String subPath = extractSubPath(stagingResultDir, fileStatus.getPath());
        if (subPath != null) {
          Path finalSubPath = new Path(finalOutputPath, subPath);
          finalSubPath = new Path(finalSubPath.getParent(), replaceFileNameSeq(finalSubPath, fileSeq, nf));
          if (!fs.exists(finalSubPath.getParent())) {
            fs.mkdirs(finalSubPath.getParent());
          }
          if (fs.exists(finalSubPath)) {
            throw new IOException("Already exists data file:" + finalSubPath);
          }
          boolean success = fs.rename(fileStatus.getPath(), finalSubPath);
          if (success) {
            LOG.info("Moving staging file[" + fileStatus.getPath() + "] + " +
                "to final output[" + finalSubPath + "]");
          } else {
            LOG.error("Can't move staging file[" + fileStatus.getPath() + "] + " +
                "to final output[" + finalSubPath + "]");
          }
        }
      }
    }

    private String extractSubPath(Path parentPath, Path childPath) {
      String parentPathStr = parentPath.toUri().getPath();
      String childPathStr = childPath.toUri().getPath();

      if (parentPathStr.length() > childPathStr.length()) {
        return null;
      }

      int index = childPathStr.indexOf(parentPathStr);
      if (index != 0) {
        return null;
      }

      return childPathStr.substring(parentPathStr.length() + 1);
    }

    private static interface QueryHook {
      boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir);
      void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext, Query query,
                   ExecutionBlockId finalExecBlockId, Path finalOutputDir) throws Exception;
    }

    private class QueryHookExecutor {
      private List<QueryHook> hookList = TUtil.newList();
      private QueryMaster.QueryMasterContext context;

      public QueryHookExecutor(QueryMaster.QueryMasterContext context) {
        this.context = context;
        hookList.add(new MaterializedResultHook());
        hookList.add(new CreateTableHook());
        hookList.add(new InsertTableHook());
      }

      public void execute(QueryContext queryContext, Query query,
                          ExecutionBlockId finalExecBlockId,
                          Path finalOutputDir) throws Exception {
        for (QueryHook hook : hookList) {
          if (hook.isEligible(queryContext, query, finalExecBlockId, finalOutputDir)) {
            hook.execute(context, queryContext, query, finalExecBlockId, finalOutputDir);
          }
        }
      }
    }

    private class MaterializedResultHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        NodeType type = lastStage.getBlock().getPlan().getType();
        return type != NodeType.CREATE_TABLE && type != NodeType.INSERT;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext,
                          Query query, ExecutionBlockId finalExecBlockId,
                          Path finalOutputDir) throws Exception {
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        TableMeta meta = lastStage.getTableMeta();

        String nullChar = queryContext.get(SessionVars.NULL_CHAR);
        meta.putOption(StorageConstants.CSVFILE_NULL, nullChar);

        TableStats stats = lastStage.getResultStats();

        TableDesc resultTableDesc =
            new TableDesc(
                query.getId().toString(),
                lastStage.getSchema(),
                meta,
                finalOutputDir);
        resultTableDesc.setExternal(true);

        stats.setNumBytes(getTableVolume(query.systemConf, finalOutputDir));
        resultTableDesc.setStats(stats);
        query.setResultDesc(resultTableDesc);
      }
    }

    private class CreateTableHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        return lastStage.getBlock().getPlan().getType() == NodeType.CREATE_TABLE;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext,
                          Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir) throws Exception {
        CatalogService catalog = context.getWorkerContext().getCatalog();
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        TableStats stats = lastStage.getResultStats();

        CreateTableNode createTableNode = (CreateTableNode) lastStage.getBlock().getPlan();
        TableMeta meta = new TableMeta(createTableNode.getStorageType(), createTableNode.getOptions());

        TableDesc tableDescTobeCreated =
            new TableDesc(
                createTableNode.getTableName(),
                createTableNode.getTableSchema(),
                meta,
                finalOutputDir);
        tableDescTobeCreated.setExternal(createTableNode.isExternal());

        if (createTableNode.hasPartition()) {
          tableDescTobeCreated.setPartitionMethod(createTableNode.getPartitionMethod());
        }

        stats.setNumBytes(getTableVolume(query.systemConf, finalOutputDir));
        tableDescTobeCreated.setStats(stats);
        query.setResultDesc(tableDescTobeCreated);

        catalog.createTable(tableDescTobeCreated);
      }
    }

    private class InsertTableHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        return lastStage.getBlock().getPlan().getType() == NodeType.INSERT;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext,
                          Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir)
          throws Exception {

        CatalogService catalog = context.getWorkerContext().getCatalog();
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        TableMeta meta = lastStage.getTableMeta();
        TableStats stats = lastStage.getResultStats();

        InsertNode insertNode = (InsertNode) lastStage.getBlock().getPlan();

        TableDesc finalTable;
        if (insertNode.hasTargetTable()) {
          String tableName = insertNode.getTableName();
          finalTable = catalog.getTableDesc(tableName);
        } else {
          String tableName = query.getId().toString();
          finalTable = new TableDesc(tableName, lastStage.getSchema(), meta, finalOutputDir);
        }

        long volume = getTableVolume(query.systemConf, finalOutputDir);
        stats.setNumBytes(volume);
        finalTable.setStats(stats);

        if (insertNode.hasTargetTable()) {
          catalog.dropTable(insertNode.getTableName());
          catalog.createTable(finalTable);
        }

        query.setResultDesc(finalTable);
      }
    }
  }

  public static long getTableVolume(TajoConf systemConf, Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(systemConf);
    ContentSummary directorySummary = fs.getContentSummary(tablePath);
    return directorySummary.getLength();
  }

  public static class SubQueryCompletedTransition implements SingleArcTransition<Query, QueryEvent> {

    private boolean hasNext(Query query) {
      ExecutionBlockCursor cursor = query.getExecutionBlockCursor();
      ExecutionBlock nextBlock = cursor.peek();
      return !query.getPlan().isTerminal(nextBlock);
    }

    private void executeNextBlock(Query query) {
      ExecutionBlockCursor cursor = query.getExecutionBlockCursor();
      ExecutionBlock nextBlock = cursor.nextBlock();
      SubQuery nextSubQuery = new SubQuery(query.context, query.getPlan(), nextBlock, query.sm);
      nextSubQuery.setPriority(query.priority--);
      query.addSubQuery(nextSubQuery);
      nextSubQuery.handle(new SubQueryEvent(nextSubQuery.getId(), SubQueryEventType.SQ_INIT));

      LOG.info("Scheduling SubQuery:" + nextSubQuery.getId());
      if(LOG.isDebugEnabled()) {
        LOG.debug("Scheduling SubQuery's Priority: " + nextSubQuery.getPriority());
        LOG.debug("Scheduling SubQuery's Plan: \n" + nextSubQuery.getBlock().getPlan());
      }
    }

    @Override
    public void transition(Query query, QueryEvent event) {
      try {
        query.completedSubQueryCount++;
        SubQueryCompletedEvent castEvent = (SubQueryCompletedEvent) event;

        if (castEvent.getState() == SubQueryState.SUCCEEDED) {
          query.successedSubQueryCount++;
        } else if (castEvent.getState() == SubQueryState.KILLED) {
          query.killedSubQueryCount++;
        } else if (castEvent.getState() == SubQueryState.FAILED) {
          query.failedSubQueryCount++;
        } else if (castEvent.getState() == SubQueryState.ERROR) {
          query.erroredSubQueryCount++;
        } else {
          LOG.error(String.format("Invalid SubQuery (%s) State %s at %s",
              castEvent.getExecutionBlockId().toString(), castEvent.getState().name(), query.getSynchronizedState().name()));
          query.eventHandler.handle(new QueryEvent(event.getQueryId(), QueryEventType.INTERNAL_ERROR));
        }

        // if a subquery is succeeded and a query is running
        if (castEvent.getState() == SubQueryState.SUCCEEDED &&  // latest subquery succeeded
            query.getSynchronizedState() == QueryState.QUERY_RUNNING &&     // current state is not in KILL_WAIT, FAILED, or ERROR.
            hasNext(query)) {                                   // there remains at least one subquery.
          query.getSubQuery(castEvent.getExecutionBlockId()).waitingIntermediateReport();
          executeNextBlock(query);
        } else { // if a query is completed due to finished, kill, failure, or error
          query.eventHandler.handle(new QueryCompletedEvent(castEvent.getExecutionBlockId(), castEvent.getState()));
        }
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        query.eventHandler.handle(new QueryEvent(event.getQueryId(), QueryEventType.INTERNAL_ERROR));
      }
    }
  }

  private static class DiagnosticsUpdateTransition implements SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      query.addDiagnostic(((QueryDiagnosticsUpdateEvent) event).getDiagnosticUpdate());
    }
  }

  private static class KillNewQueryTransition implements SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      query.setFinishTime();
      query.eventHandler.handle(new QueryMasterQueryCompletedEvent(query.getId()));
    }
  }

  private static class KillSubQueriesTransition implements SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      synchronized (query.subqueries) {
        for (SubQuery subquery : query.subqueries.values()) {
          query.eventHandler.handle(new SubQueryEvent(subquery.getId(), SubQueryEventType.SQ_KILL));
        }
      }
    }
  }

  private static class InternalErrorTransition implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent event) {
      query.setFinishTime();
      query.eventHandler.handle(new QueryMasterQueryCompletedEvent(query.getId()));
    }
  }

  @Override
  public void handle(QueryEvent event) {
    LOG.info("Processing " + event.getQueryId() + " of type " + event.getType());
    try {
      writeLock.lock();
      QueryState oldState = getSynchronizedState();
      try {
        getStateMachine().doTransition(event.getType(), event);
        queryState = getSynchronizedState();
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state"
            + ", type:" + event
            + ", oldState:" + oldState.name()
            + ", nextState:" + getSynchronizedState().name()
            , e);
        eventHandler.handle(new QueryEvent(this.id, QueryEventType.INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (oldState != getSynchronizedState()) {
        LOG.info(id + " Query Transitioned from " + oldState + " to " + getSynchronizedState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
