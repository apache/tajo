package tajo.worker.dataserver;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import tajo.worker.dataserver.retriever.DataRetriever;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * @author Hyunsik Choi
 */
public class HttpDataServerPipelineFactory implements ChannelPipelineFactory {
  private final DataRetriever ret;

  public HttpDataServerPipelineFactory(DataRetriever ret) {
    this.ret = ret;
  }

  public ChannelPipeline getPipeline() throws Exception {
    // Create a default pipeline implementation.
    ChannelPipeline pipeline = pipeline();

    // Uncomment the following line if you want HTTPS
    // SSLEngine engine =
    // SecureChatSslContextFactory.getServerContext().createSSLEngine();
    // engine.setUseClientMode(false);
    // pipeline.addLast("ssl", new SslHandler(engine));

    pipeline.addLast("decoder", new HttpRequestDecoder());
    //pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
    pipeline.addLast("encoder", new HttpResponseEncoder());
    pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
    //pipeline.addLast("deflater", new HttpContentCompressor());
    pipeline.addLast("handler", new HttpDataServerHandler(ret));
    return pipeline;
  }
}
