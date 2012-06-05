/**
 * 
 */
package tajo.worker.dataserver.retriever;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;

import tajo.worker.dataserver.FileAccessForbiddenException;
import tajo.worker.dataserver.HttpDataServerHandler;

/**
 * @author Hyunsik Choi
 */
public class DirectoryRetriever implements DataRetriever {
  public String baseDir;
  
  public DirectoryRetriever(String baseDir) {
    this.baseDir = baseDir;
  }

  @Override
  public FileChunk handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException {
    final String path = HttpDataServerHandler.sanitizeUri(request.getUri());
    if (path == null) {
      throw new IllegalArgumentException("Wrong path: " +path);
    }

    File file = new File(baseDir, path);
    if (file.isHidden() || !file.exists()) {
      throw new FileNotFoundException("No such file: " + baseDir + "/" + path);
    }
    if (!file.isFile()) {
      throw new FileAccessForbiddenException("No such file: " 
          + baseDir + "/" + path); 
    }
    
    return new FileChunk(file, 0, file.length());
  }
}
