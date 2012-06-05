package nta.engine;

import tajo.worker.dataserver.retriever.FileChunk;
import tajo.worker.dataserver.retriever.RetrieverHandler;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public class PartitionRetrieverHandler implements RetrieverHandler {
  private final String baseDir;

  public PartitionRetrieverHandler(String baseDir) {
    this.baseDir = baseDir;
  }

  @Override
  public FileChunk get(Map<String, String> kvs) throws IOException {
    // nothing to verify the file because AdvancedDataRetriever checks
    // its validity of the file.
    File file = new File(baseDir + "/" + kvs.get("fn"));

    return new FileChunk(file, 0, file.length());
  }
}
