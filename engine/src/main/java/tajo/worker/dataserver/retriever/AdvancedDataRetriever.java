/**
 * 
 */
package tajo.worker.dataserver.retriever;

import com.google.common.collect.Maps;
import nta.engine.QueryUnitId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import tajo.worker.dataserver.FileAccessForbiddenException;
import tajo.worker.dataserver.HttpUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public class AdvancedDataRetriever implements DataRetriever {
  private final Log LOG = LogFactory.getLog(AdvancedDataRetriever.class);
  private final Map<String, RetrieverHandler> handlerMap = Maps.newConcurrentMap();

  public AdvancedDataRetriever() {
  }
  
  public void register(QueryUnitId id, RetrieverHandler handler) {
    synchronized (handlerMap) {
      if (!handlerMap.containsKey(id.toString())) {
        handlerMap.put(id.toString(), handler);
      }
    } 
  }
  
  public void unregister(QueryUnitId id) {
    synchronized (handlerMap) {
      if (handlerMap.containsKey(id.toString())) {
        handlerMap.remove(id.toString());
      }
    }
  }

  /* (non-Javadoc)
   * @see tajo.worker.dataserver.retriever.DataRetriever#handle(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.handler.codec.http.HttpRequest)
   */
  @Override
  public FileChunk handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException {
    int startIdx = request.getUri().indexOf('?');
    if (startIdx < 0) {
      throw new IllegalArgumentException("Wrong request: " + request.getUri());
    }
    
    String query = request.getUri().substring(startIdx + 1);
    LOG.info("QUERY: " + query);

    Map<String,String> params = HttpUtil.getParamsFromQuery(query);

    if (!params.containsKey("qid")) {
      throw new FileNotFoundException("No such qid: " + params.containsKey("qid"));
    }
    
    RetrieverHandler handler = handlerMap.get(params.get("qid"));
    FileChunk chunk = handler.get(params);
    if (chunk == null) {
      if (params.containsKey("qid")) { // if there is no content corresponding to the query
        return null;
      } else { // if there is no
        throw new FileNotFoundException("No such a file corresponding to " + params.get("qid"));
      }
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