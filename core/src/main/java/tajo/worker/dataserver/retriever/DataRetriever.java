package tajo.worker.dataserver.retriever;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public interface DataRetriever {  
  FileChunk [] handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException;
}
