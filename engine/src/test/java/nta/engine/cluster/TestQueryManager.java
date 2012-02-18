/**
 * 
 */
package nta.engine.cluster;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import nta.engine.QueryIdFactory;
import nta.engine.LeafServerProtos.QueryStatus;
import nta.engine.QueryUnitProtos.InProgressStatus;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author jihoon
 *
 */
public class TestQueryManager {
  QueryManager qm;
  
  class ProgressUpdator extends Thread {

    @Override
    public void run() {
      InProgressStatus.Builder builder = InProgressStatus.newBuilder();
      try {
        for (int i = 0; i < 3; i++) {
          Thread.sleep(1000);
          builder.setId("test"+i);
          builder.setProgress(i/3.f);
          builder.setStatus(QueryStatus.INPROGRESS);
          qm.updateProgress(QueryIdFactory.newQueryUnitId(), 
              builder.build());
        }
      } catch (InterruptedException e) {
        
      } finally {
      }
    }
    
  }

  @Before
  public void setup() {
    QueryIdFactory.reset();
    qm = new QueryManager();
  }
  
  @Test
  public void testUpdateProgress() throws InterruptedException {
    ProgressUpdator[] pu = new ProgressUpdator[2];
    for (int i = 0; i < pu.length; i++) {
      pu[i] = new ProgressUpdator();
      pu[i].start();
    }
    for (int i = 0; i < pu.length; i++) {
      pu[i].join();
    }
    assertEquals(qm.getAllProgresses().size(), 6);
  }
}
