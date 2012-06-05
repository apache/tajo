/**
 * 
 */
package tajo.worker.dataserver.retriever;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import nta.engine.QueryUnitId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import tajo.worker.dataserver.FileAccessForbiddenException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @author Hyunsik Choi
 */
public class AdvancedDataRetriever implements DataRetriever {
  private final Log LOG = LogFactory.getLog(AdvancedDataRetriever.class);
  private final Map<String, RetrieverHandler> map = Maps.newConcurrentMap();

  public AdvancedDataRetriever() {
  }
  
  public void register(QueryUnitId id, RetrieverHandler handler) {
    synchronized (map) {
      if (!map.containsKey(id.toString())) {
        map.put(id.toString(), handler);
      }
    } 
  }
  
  public void unregister(QueryUnitId id) {
    synchronized (map) {
      if (map.containsKey(id.toString())) {
        map.remove(id.toString());
      }
    }
  }

  /* (non-Javadoc)
   * @see tajo.worker.dataserver.retriever.DataRetriever#handle(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.handler.codec.http.HttpRequest)
   */
  @Override
  public FileChunk handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException {
       
    int start = request.getUri().indexOf('?');
    if (start < 0) {
      throw new IllegalArgumentException("Wrong request: " + request.getUri());
    }
    
    String queryStr = request.getUri().substring(start + 1);
    LOG.info("QUERY: " + queryStr);
    String [] queries = queryStr.split("&");

    Map<String,String> kvs = Maps.newHashMap();
    String [] param = null;
    for (String query : queries) {
      param = query.split("=");
      kvs.put(param[0], param[1]);
    }

    if (!kvs.containsKey("qid")) {
      throw new FileNotFoundException("No such qid: " + kvs.containsKey("qid"));
    }
    
    RetrieverHandler handler = map.get(kvs.get("qid"));
    FileChunk chunk = handler.get(kvs);
    if (chunk == null) {
      throw new FileNotFoundException("No such a file corresponding to " + kvs.containsKey("qid"));
    }

    File file = chunk.getFile();
    if (file.isHidden() || !file.exists()) {
      throw new FileNotFoundException("No such file: " + file.getAbsolutePath());
    }
    if (!file.isFile()) {
      throw new FileAccessForbiddenException(file.getAbsolutePath() + " is not file");
    }
    
    return chunk;
  }
}