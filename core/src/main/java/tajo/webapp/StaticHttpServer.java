package tajo.webapp;

import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.Connector;
import tajo.engine.NConstants;
import tajo.engine.TajoMaster;

import java.io.IOException;

public class StaticHttpServer extends HttpServer{
  private static StaticHttpServer instance = null;
  private TajoMaster master = null;
  
  private StaticHttpServer(TajoMaster master , String name, String bindAddress, int port,
      boolean findPort, Connector connector, Configuration conf,
      String[] pathSpecs) throws IOException {
    super( name, bindAddress, port, findPort, connector, conf, pathSpecs);
    this.master = master;
  }
  public static StaticHttpServer getInstance() {
    return instance;
  }
  public static StaticHttpServer getInstance( TajoMaster master, String name,
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
  public TajoMaster getMaster() {
    
    return this.master;
  }
  public void set(String name, Object object) {
    instance.setAttribute(name, object);
  }
}
