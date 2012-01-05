package nta.rpc;

import java.net.InetSocketAddress;

import nta.rpc.NettyRpc;
import nta.rpc.WritableRpcServer;
import nta.rpc.TestWritableAsyncRpc.DummyServer;
import nta.rpc.TestWritableAsyncRpc.DummyServerInterface;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestWritableBlockingRpc {
  private static String MESSAGE = "TestWritableBlockingRpc";

  @Test
  public void testRpc() throws Exception {
    WritableRpcServer server = NettyRpc.getWritableAsyncRpcServer(
        new DummyServer(), new InetSocketAddress(10002));
    server.start();

    DummyServerInterface proxy = (DummyServerInterface) NettyRpc
        .getWritableBlockingRpcProxy(DummyServerInterface.class,
            new InetSocketAddress(10002));

    DoubleWritable re1 = proxy.sum(new IntWritable(1), new LongWritable(2),
        new DoubleWritable(3.15d), new FloatWritable(2f));
    assertEquals(new DoubleWritable(8.15d), re1);

    Text res2 = proxy.echo(new Text(MESSAGE));
    assertEquals(new Text(MESSAGE), res2);

    server.shutdown();
  }
}
