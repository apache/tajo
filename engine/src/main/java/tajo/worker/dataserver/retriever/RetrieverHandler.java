package tajo.worker.dataserver.retriever;

import java.io.IOException;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public interface RetrieverHandler {
  public FileChunk get(Map<String, String> kvs) throws IOException;
}
