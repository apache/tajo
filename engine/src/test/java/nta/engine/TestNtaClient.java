package nta.engine;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.rpc.Callback;
import nta.rpc.RemoteException;
import nta.storage.CSVFile2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNtaClient {
  NtaClient cli = null;

  private static NtaTestingUtility util;
  private static NtaConf conf;
  private static NtaEngineMaster master;

  @Before
  public void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniCluster(1);

    master = util.getMiniNtaEngineCluster().getMaster();
    InetSocketAddress serverAddr = master.getRpcServerAddr();
    String ip = serverAddr.getAddress().getHostAddress();
    int port = serverAddr.getPort();
    cli = new NtaClient(ip, port);
    
    
  }

  @After
  public void tearDown() throws Exception {
    cli.close();
    util.shutdownMiniCluster();
  }

  @Test
  public void testSubmit() throws IOException, InterruptedException, ExecutionException {
    try {
      String resultSetPath = cli.executeQuery("");
      System.out.println(resultSetPath);
      
      Callback<String> cb = new Callback<String>();
      cli.executeQueryAsync(cb, "");
      
      String s = (String)cb.get();
      System.out.println(s);
      
      Schema schema = new Schema();
      schema.addColumn("name", DataType.STRING);
      schema.addColumn("id", DataType.INT);

      TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
      meta.putOption(CSVFile2.DELIMITER, ",");

      cli.attachTable("attach1", "target/testdata/TestNtaClient/attach1");
      assertTrue(cli.existsTable("attach1"));

      cli.detachTable("attach1");
      assertFalse(cli.existsTable("attach1"));

    } catch (RemoteException e) {
      System.out.println(e.getMessage());
    }
    
  }
}
