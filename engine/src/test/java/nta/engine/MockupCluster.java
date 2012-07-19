package nta.engine;

import com.google.common.collect.Lists;
import nta.catalog.TConstants;
import nta.conf.NtaConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * @author Jihoon Son
 */
public class MockupCluster {
  static final Log LOG = LogFactory.getLog(MockupCluster.class);

  private NtaEngineMaster master;
  private List<MockupWorker> workers;
  private final Configuration conf;
  private final int numWorkers;
  private final int numNormalWorkers;
  private final int numAbortWorkers;
  private final int numShutdownWorkers;

  public MockupCluster(int numWorkers) throws Exception {
    this(numWorkers, 0, 0);
  }

  public MockupCluster(int numWorkers,
                       int numAbortWorkers, int numShutdownWorkers)
      throws Exception {
    this.conf = new NtaConf();
    this.conf.set(NConstants.MASTER_ADDRESS, "localhost:0");
    this.conf.set(NConstants.CATALOG_ADDRESS, "localhost:0");
    this.conf.set(NConstants.LEAFSERVER_PORT, "0");

    this.numWorkers = numWorkers;
    this.numAbortWorkers = numAbortWorkers;
    this.numShutdownWorkers = numShutdownWorkers;
    this.numNormalWorkers = numWorkers - (numAbortWorkers + numShutdownWorkers);
    this.master = new NtaEngineMaster(conf);

    this.workers = Lists.newArrayList();

    int i;
    for (i = 0; i < numNormalWorkers; i++) {
      workers.add(new MockupNormalWorker(conf));
    }
    for (i = 0; i < numAbortWorkers; i++) {
      workers.add(new MockupAbortWorker(conf));
    }
    for (i = 0; i < numShutdownWorkers; i++) {
      workers.add(new MockupShutdownWorker(conf, 5000));
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  public NtaEngineMaster getMaster() {
    return this.master;
  }

  public List<MockupWorker> getWorkers() {
    return this.workers;
  }

  public MockupWorker getWorker(int index) {
    return this.workers.get(index);
  }

  public int getClusterSize() {
    return workers.size();
  }

  public void start() {
    master.start();
    while (true) {
      if (master.isMasterRunning()) {
        break;
      }
    }

    for (MockupWorker worker : workers) {
      worker.start();
    }
  }

  public void shutdown() {
    if (this.workers != null) {
      for(MockupWorker t: this.workers) {
        if (t.isAlive()) {
          try {
            t.shutdown("Shutdown");
            t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }
    master.shutdown();
  }

  public void join() {
    if (this.workers != null) {
      for(Thread t: this.workers) {
        if (t.isAlive()) {
          try {
            t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }

    if(master.isAlive()) {
      try {
        master.join();
      } catch (InterruptedException e) {
        // continue
      }
    }
  }
}
