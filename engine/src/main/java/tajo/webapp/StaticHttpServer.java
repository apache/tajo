package tajo.webapp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.Connector;

import nta.engine.NConstants;
import nta.engine.NtaEngineMaster;

public class StaticHttpServer extends HttpServer{
  private static StaticHttpServer instance = null;
  private NtaEngineMaster master = null;
  
  private StaticHttpServer(NtaEngineMaster master , String name, String bindAddress, int port,
      boolean findPort, Connector connector, Configuration conf,
      String[] pathSpecs) throws IOException {
    super( name, bindAddress, port, findPort, connector, conf, pathSpecs);
    this.master = master;
  }
  public static StaticHttpServer getInstance() {
    return instance;
  }
  public static StaticHttpServer getInstance( NtaEngineMaster master, String name,
      String bindAddress, int port, boolean findPort, Connector connector, Configuration conf,
      String[] pathSpecs) throws IOException {
    String addr = bindAddress;
    if(instance == null) {
      if(bindAddress == null || bindAddress.compareTo("") == 0) {
        addr = conf.get(NConstants.MASTER_ADDRESS).split(":")[0];
      }
      
      instance = new StaticHttpServer(master, name, addr, port,
          findPort, connector, conf, pathSpecs);
      instance.setAttribute("tajo.master", master);
      instance.setAttribute("tajo.master.addr", addr);
      instance.setAttribute("tajo.master.conf", conf);
      instance.setAttribute("tajo.master.starttime", System.currentTimeMillis());
    }
    return instance;
  }
  public NtaEngineMaster getMaster() {
    
    return this.master;
  }
  public void set(String name, Object object) {
    instance.setAttribute(name, object);
  }
}
