package nta.engine;

import com.google.common.collect.Lists;
import nta.engine.cluster.*;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.planner.global.MasterPlan;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.QueryUnitAttempt;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.MasterInterfaceProtos.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author jihoon
 */
public class ScheduleUnitExecutorTest {
  private final static Log LOG = LogFactory.getLog(ScheduleUnitExecutor.class);

  ScheduleUnitExecutor.QueryUnitSubmitter submitter;
  QueryManager qm;
  WorkerListener wl;
  int testSize = 12000;
  int sleepTime = 2000;
  List<QueryUnit> queryUnits;
  Random random = new Random();

  @Before
  public void setup() throws NoSuchQueryIdException, IOException {
    QueryIdFactory.reset();
    Configuration conf = new Configuration();
    qm = new QueryManager();
    ClusterManager cm = new DummyClusterManager(null, conf, null);
    wl = new WorkerListener(conf, qm, null);
    wl.start();

    QueryId qid = QueryIdFactory.newQueryId();
    Query query = new Query(qid, "test query");
    qm.addQuery(query);
    SubQueryId subId = QueryIdFactory.newSubQueryId(qid);
    SubQuery subQuery = new SubQuery(subId);
    qm.addSubQuery(subQuery);

    ScheduleUnitId suid = QueryIdFactory.newScheduleUnitId(subId);
    ScheduleUnit sunit = new ScheduleUnit(suid);
    qm.addScheduleUnit(sunit);

    queryUnits = Lists.newArrayList();
    for (int i = 0; i < testSize; i++) {
      QueryUnitId qunitid = QueryIdFactory.newQueryUnitId(suid);
      queryUnits.add(new QueryUnit(qunitid));
    }
    sunit.setQueryUnits(queryUnits.toArray(new QueryUnit[queryUnits.size()]));

    submitter = new ScheduleUnitExecutor(conf,
        null, null, cm,
        qm, null, new MasterPlan(sunit)).getSubmitter();
    for (QueryUnit unit : queryUnits) {
      QueryUnitAttempt attempt = unit.newAttempt();
      attempt.setHost("test" + random.nextInt(testSize));
      submitter.submitQueryUnit(attempt);
    }
  }

  @After
  public void terminate() {
    wl.shutdown();
  }

//  @Test
  public void testConcurrentUpdate() throws Exception, ExecutionException {
    ExecutorService executorService = Executors.newFixedThreadPool(testSize);
    List<Future<Long>> ltr = Lists.newArrayList();

    int workerNum = 0;
    for (int i = 0; i < queryUnits.size(); i+=120) {
      DummyWorker tr = new DummyWorker(queryUnits.subList(i, i + 120));
      ltr.add(executorService.submit(tr));
      workerNum++;
    }

    LOG.info(workerNum + " workers are started");

    int finished = 0;
    while (true) {
      Thread.sleep(sleepTime);
      finished += submitter.updateSubmittedQueryUnitStatus();
      if (submitter.getSubmittedNum() == 0) {
        break;
      }
    }
    assertEquals(queryUnits.size(), finished);
  }

  public class DummyWorker implements Callable<Long> {
    List<QueryUnit> queryunits;

    public DummyWorker(List<QueryUnit> queryunits) throws InterruptedException {
      this.queryunits = queryunits;
    }

    private void sendHeartBeat(QueryStatus status) {
      PingRequestProto.Builder ping =  PingRequestProto.newBuilder();
      ping.setTimestamp(0);
      ping.setServerName("test" + random.nextInt(testSize));
      for (QueryUnit unit : queryunits) {
        InProgressStatusProto.Builder builder =
            InProgressStatusProto.newBuilder();
        builder.setId(unit.getLastAttempt().getId().getProto())
            .setProgress(0)
            .setStatus(status);
        ping.addStatus(builder.build());
      }

      wl.reportQueryUnit(ping.build());
    }

    @Override
    public Long call() throws Exception {
      Thread.sleep(sleepTime);
      sendHeartBeat(QueryStatus.QUERY_INITED);
      Thread.sleep(sleepTime);
      sendHeartBeat(QueryStatus.QUERY_PENDING);
      for (int i = 0; i < 100; i++) {
        Thread.sleep(sleepTime);
        sendHeartBeat(QueryStatus.QUERY_INPROGRESS);
      }
      Thread.sleep(sleepTime);
      sendHeartBeat(QueryStatus.QUERY_FINISHED);
      return new Long(0);
    }
  }

  public class DummyClusterManager extends ClusterManager {

    public DummyClusterManager(WorkerCommunicator wc, Configuration conf, LeafServerTracker tracker) throws IOException {
      super(wc, conf, tracker);
    }
  }
}
