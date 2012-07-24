package tajo.worker.dataserver.retriever;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public interface RetrieverHandler {
  /**
   *
   * @param params url-decoded params
   * @return a desired part of a file
   * @throws IOException
   */
  public FileChunk [] get(Map<String, List<String>> params) throws IOException;
}
