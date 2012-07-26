package nta.rpc;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

import com.google.protobuf.MessageLite;

public class ProtoPipelineFactory implements ChannelPipelineFactory {
  private final ChannelUpstreamHandler handler;
  private final MessageLite defaultInstance;

  public ProtoPipelineFactory(ChannelUpstreamHandler handlerFactory,
      MessageLite defaultInstance) {
    this.handler = handlerFactory;
    this.defaultInstance = defaultInstance;
  }

  public ChannelPipeline getPipeline() throws Exception {
    ChannelPipeline p = Channels.pipeline();
    p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576*2, 0, 4,
        0, 4));
    p.addLast("protobufDecoder", new ProtobufDecoder(defaultInstance));
    p.addLast("frameEncoder", new LengthFieldPrepender(4));
    p.addLast("protobufEncoder", new ProtobufEncoder());
    p.addLast("handler", handler);
    return p;
  }
}
