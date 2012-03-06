package tajo.worker.dataserver;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Random;

import nta.engine.EngineTestingUtils;

import org.apache.hadoop.net.NetUtils;
import org.junit.Before;
import org.junit.Test;

import tajo.worker.dataserver.retriever.DataRetriever;
import tajo.worker.dataserver.retriever.DirectoryRetriever;

/**
 * @author Hyunsik Choi
 */
public class TestHttpDataServer {
  private String TEST_DATA = "target/test-data/TestHttpDataServer";
  
  @Before
  public void setUp() throws Exception {
    EngineTestingUtils.buildTestDir(TEST_DATA);
  }

  @Test
  public final void testHttpDataServer() throws Exception {
    Random rnd = new Random();
    FileWriter writer = new FileWriter(TEST_DATA+"/"+"testHttp");
    String watermark = "test_"+rnd.nextInt();
    writer.write(watermark);
    writer.flush();
    writer.close();

    DataRetriever ret = new DirectoryRetriever(TEST_DATA);
    HttpDataServer server = new HttpDataServer(
        NetUtils.createSocketAddr("127.0.0.1:0"), ret);
    server.start();

    InetSocketAddress addr = server.getBindAddress();
    URL url = new URL("http://127.0.0.1:"+addr.getPort() 
        + "/testHttp");
    BufferedReader in = new BufferedReader(new InputStreamReader(
        url.openStream()));
    String line = null;    
    boolean found = false;
    while ((line = in.readLine()) != null) {
      if (line.equals(watermark))
        found = true;
    }    
    assertTrue(found);
    in.close();    
    server.stop();
  }
}
