package tajo.worker.dataserver.retriever;

import java.io.IOException;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * @author Hyunsik Choi
 */
public interface DataRetriever {  
  FileChunk handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException;
}
