package tajo.worker.dataserver;

import org.apache.hadoop.thirdparty.guava.common.collect.Maps;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Hyunsik Choi
 */
public class TestHttpUtil {
  private URI uri = URI.create("http://127.0.0.1:80/?key1=val1&key2=val2");

  @Test
  public void testGetParams() throws UnsupportedEncodingException {
    Map<String,String> params = HttpUtil.getParamsFromQuery(uri.getQuery());
    assertEquals(2, params.size());
    assertEquals("val1", params.get("key1"));
    assertEquals("val2", params.get("key2"));
  }

  @Test
  public void testBuildQuery() throws UnsupportedEncodingException {
    Map<String,String> params = Maps.newTreeMap();
    params.put("key1", "val1");
    params.put("key2", "val2");
    String query = HttpUtil.buildQuery(params);
    assertEquals(uri.getQuery(), query);
  }
}
