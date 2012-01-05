package nta.rpc;

import java.net.InetSocketAddress;

import nta.rpc.Callback;
import nta.rpc.NettyRpc;
import nta.rpc.WritableRpcServer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestWritableAsyncRpc {
  public static String MESSAGE = TestWritableAsyncRpc.class.getName();

  public static interface DummyServerInterface {
    public DoubleWritable sum(IntWritable x1, LongWritable x2,
        DoubleWritable x3, FloatWritable x4);

    public Text echo(Text message);
  }

  public static interface DummyClientInterface {
    public void sum(Callback<DoubleWritable> callback, IntWritable x1,
        LongWritable x2, DoubleWritable x3, FloatWritable x4);

    public void echo(Callback<Text> callback, Text message);
  }

  public static class DummyServer implements DummyServerInterface {

    @Override
    public DoubleWritable sum(IntWritable x1, LongWritable x2,
        DoubleWritable x3, FloatWritable x4) {
      return new DoubleWritable(x1.get() + x2.get() + x3.get() + x4.get());
    }

    @Override
    public Text echo(Text message) {
      return message;
    }
  }

  DoubleWritable answer1 = null;
  Text answer2 = null;

  @Test
  public void testRpc() throws Exception {
    WritableRpcServer server = NettyRpc.getWritableAsyncRpcServer(
        new DummyServer(), new InetSocketAddress(10001));
    server.start();

    DummyClientInterface proxy = (DummyClientInterface) NettyRpc
        .getWritableAsyncRpcProxy(DummyServerInterface.class,
            DummyClientInterface.class, new InetSocketAddress(10001));

    EchoCallBack cb = new EchoCallBack();
    proxy.echo(cb, new Text(MESSAGE));

    proxy.sum(new AnswerCallback(), new IntWritable(1), new LongWritable(2),
        new DoubleWritable(3.15d), new FloatWritable(2f));

    server.shutdown();

    assertEquals(new DoubleWritable(8.15d), answer1);
    assertEquals(new Text(MESSAGE), answer2);
  }

  public class AnswerCallback implements Callback<DoubleWritable> {
    @Override
    public void onComplete(DoubleWritable response) {
      answer1 = response;
    }

    @Override
    public void onFailure(Throwable error) {
    }
  }

  public class EchoCallBack implements Callback<Text> {
    @Override
    public void onComplete(Text response) {
      answer2 = response;
    }

    @Override
    public void onFailure(Throwable error) {
    }
  }
}
