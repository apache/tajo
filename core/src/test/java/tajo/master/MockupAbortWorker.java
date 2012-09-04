package tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import tajo.QueryUnitAttemptId;
import tajo.conf.TajoConf;
import tajo.engine.MasterInterfaceProtos.Command;
import tajo.engine.MasterInterfaceProtos.PingResponseProto;
import tajo.engine.MasterInterfaceProtos.QueryStatus;

import java.io.IOException;

/**
 *
 * @author jihoon
 */
public class MockupAbortWorker extends MockupWorker {
  private final static Log LOG = LogFactory.getLog(MockupAbortWorker.class);

  public MockupAbortWorker(TajoConf conf) {
    super(conf, Type.ABORT);
  }

  private void abortTask() {
    if (taskQueue.size() > 0) {
      MockupTask task = taskQueue.remove(0);
      task.setStatus(QueryStatus.QUERY_ABORTED);
    }
  }

  @Override
  public void run() {
    try {
      prepareServing();
      participateCluster();
      if (!this.stopped) {
        long before = -1;
        long sleeptime = 3000;
        long time;
        while (!this.stopped) {
          time = System.currentTimeMillis();
          if (before == -1) {
            sleeptime = 3000;
          } else {
            sleeptime = 3000 - (time - before);
            if (sleeptime > 0) {
              sleep(sleeptime);
            }
          }
          progressTask();
          abortTask();

          PingResponseProto response = sendHeartbeat(time);
          before = time;

          QueryUnitAttemptId qid;
          MockupTask task;
          QueryStatus status;
          for (Command cmd : response.getCommandList()) {
            qid = new QueryUnitAttemptId(cmd.getId());
            if (!taskMap.containsKey(qid)) {
              LOG.error("ERROR: no such task " + qid);
              continue;
            }
            task = taskMap.get(qid);
            status = task.getStatus();

            switch (cmd.getType()) {
              case FINALIZE:
                if (status == QueryStatus.QUERY_FINISHED
                    || status == QueryStatus.QUERY_DATASERVER
                    || status == QueryStatus.QUERY_ABORTED
                    || status == QueryStatus.QUERY_KILLED) {
                  task.setStatus(QueryStatus.QUERY_FINISHED);
                } else {
                  task.setStatus(QueryStatus.QUERY_ABORTED);
                }

                break;
              case STOP:
                if (status == QueryStatus.QUERY_INPROGRESS) {
                  task.setStatus(QueryStatus.QUERY_ABORTED);
                } else {
                  LOG.error("ERROR: Illegal State of " + qid + "(" + status + ")");
                }

                break;
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    } finally {
      clear();
    }
  }
}
