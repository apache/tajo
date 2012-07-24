/**
 * 
 */
package tajo.worker.dataserver.retriever;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import nta.engine.QueryUnitId;
import nta.engine.ScheduleUnitId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
  public FileChunk [] handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException {
    int startIdx = request.getUri().indexOf('?');
    if (startIdx < 0) {
      throw new IllegalArgumentException("Wrong request: " + request.getUri());
    }

    final Map<String,List<String>> q =
      new QueryStringDecoder(request.getUri()).getParameters();

    if (!q.containsKey("qid")) {
      throw new FileNotFoundException("No such qid: " + q.containsKey("qid"));
    }

    if (q.containsKey("start") || q.containsKey("end")) {
      RetrieverHandler handler = handlerMap.get(q.get("qid").get(0));
      FileChunk [] chunk = handler.get(q);
      if (chunk == null) {
        if (q.containsKey("qid")) { // if there is no content corresponding to the query
          return null;
        } else { // if there is no
          throw new FileNotFoundException("No such a file corresponding to " + q.get("qid"));
        }
      }

      return chunk;
    } else {
      List<FileChunk> chunks = Lists.newArrayList();
      List<String> qids = splitMaps(q.get("qid"));
      for (String qid : qids) {
        QueryUnitId quid = new QueryUnitId(new ScheduleUnitId(q.get("sid").get(0)),
            Integer.parseInt(qid));
        RetrieverHandler handler = handlerMap.get(quid.toString());
        FileChunk [] chunk = handler.get(q);
        chunks.addAll(Lists.newArrayList(chunk));
      }

      return chunks.toArray(new FileChunk[chunks.size()]);
    }
  }

  private List<String> splitMaps(List<String> fns) {
    if (null == fns) {
      return null;
    }
    final List<String> ret = new ArrayList<String>();
    for (String s : fns) {
      Collections.addAll(ret, s.split(","));
    }
    return ret;
  }
}