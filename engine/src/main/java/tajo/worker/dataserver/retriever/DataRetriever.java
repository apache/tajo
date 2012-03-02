package tajo.worker.dataserver.retriever;

import java.io.File;
import java.io.IOException;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * @author Hyunsik Choi
 */
public interface DataRetriever {  
  File handle(ChannelHandlerContext ctx, HttpRequest request) 
      throws IOException;
}
