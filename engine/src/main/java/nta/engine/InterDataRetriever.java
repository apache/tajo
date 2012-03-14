/**
 * 
 */
package nta.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;

import tajo.worker.dataserver.FileAccessForbiddenException;
import tajo.worker.dataserver.retriever.DataRetriever;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author Hyunsik Choi
 */
public class InterDataRetriever implements DataRetriever {
  private final Log LOG = LogFactory.getLog(InterDataRetriever.class);
  private Set<QueryUnitId> registered = Sets.newHashSet();
  private Map<String, String> map = Maps.newConcurrentMap();

  public InterDataRetriever() {
  }
  
  public void register(QueryUnitId id, String workDir) {
    synchronized (registered) {
      if (!registered.contains(id)) {      
        map.put(id.toString(), workDir);
        registered.add(id);      
      }
    } 
  }
  
  public void unregister(QueryUnitId id) {
    synchronized (registered) {
      if (registered.contains(id)) {
        map.remove(id.toString());
        registered.remove(id);
      }
    }
  }

  /* (non-Javadoc)
   * @see tajo.worker.dataserver.retriever.DataRetriever#handle(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.handler.codec.http.HttpRequest)
   */
  @Override
  public File handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException {
       
    int start = request.getUri().indexOf('?');
    if (start < 0) {
      throw new IllegalArgumentException("Wrong request: " + request.getUri());
    }
    
    String queryStr = request.getUri().substring(start + 1);
    LOG.info("QUERY: " + queryStr);
    String [] queries = queryStr.split("&");
    
    String qid = null;
    String fn = null;
    String [] kv = null;
    for (String query : queries) {
      kv = query.split("=");
      if (kv[0].equals("qid")) {
        qid = kv[1];
      } else if (kv[0].equals("fn")) {
        fn = kv[1];
      }
    }
    
    String baseDir = map.get(qid);
    if (baseDir == null) {
      throw new FileNotFoundException("No such qid: " + qid.toString());
    }
    
    File file = new File(baseDir, fn);
    if (file.isHidden() || !file.exists()) {
      throw new FileNotFoundException("No such file: " + baseDir + "/" 
          + file.getName());
    }
    if (!file.isFile()) {
      throw new FileAccessForbiddenException("No such file: " 
          + baseDir + "/" + file.getName()); 
    }
    
    return file;
  }
}