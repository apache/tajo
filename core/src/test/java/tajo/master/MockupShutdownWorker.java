package tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import tajo.QueryUnitAttemptId;
import tajo.conf.TajoConf;
import tajo.engine.MasterInterfaceProtos;

import java.io.IOException;

/**
 * @author jihoon
 */
public class MockupShutdownWorker extends MockupWorker {
  private final static Log LOG = LogFactory.getLog(MockupShutdownWorker.class);
  private int lifetime;

  public MockupShutdownWorker(final TajoConf conf, final int lifetime) {
    super(conf, Type.SHUTDOWN);
    this.lifetime = lifetime;
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
          lifetime -= 3000;
          if (lifetime <= 0) {
            shutdown("shutdown test");
            break;
          }
          progressTask();

          MasterInterfaceProtos.PingResponseProto response = sendHeartbeat(time);
          before = time;

          QueryUnitAttemptId qid;
          MockupTask task;
          MasterInterfaceProtos.QueryStatus status;
          for (MasterInterfaceProtos.Command cmd : response.getCommandList()) {
            qid = new QueryUnitAttemptId(cmd.getId());
            if (!taskMap.containsKey(qid)) {
              LOG.error("ERROR: no such task " + qid);
              continue;
            }
            task = taskMap.get(qid);
            status = task.getStatus();

            switch (cmd.getType()) {
              case FINALIZE:
                if (status == MasterInterfaceProtos.QueryStatus.QUERY_FINISHED
                    || status == MasterInterfaceProtos.QueryStatus.QUERY_DATASERVER
                    || status == MasterInterfaceProtos.QueryStatus.QUERY_ABORTED
                    || status == MasterInterfaceProtos.QueryStatus.QUERY_KILLED) {
                  task.setStatus(MasterInterfaceProtos.QueryStatus.QUERY_FINISHED);
                } else {
                  task.setStatus(MasterInterfaceProtos.QueryStatus.QUERY_ABORTED);
                }

                break;
              case STOP:
                if (status == MasterInterfaceProtos.QueryStatus.QUERY_INPROGRESS) {
                  task.setStatus(MasterInterfaceProtos.QueryStatus.QUERY_ABORTED);
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
    }finally {
      clear();
    }
  }
}
