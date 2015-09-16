package org.apache.tajo.util;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.RpcConstants;
import org.junit.Test;

import java.util.Properties;

import static org.apache.tajo.rpc.RpcConstants.CLIENT_CONNECTION_TIMEOUT;
import static org.apache.tajo.rpc.RpcConstants.RPC_RETRY_NUM;
import static org.junit.Assert.*;

public class TestRpcConnectionParamUtil {

  @Test
  public void testGetDefaults() throws Exception {
    TajoConf conf = new TajoConf();
    Properties defaultParams = RpcConnectionParamUtil.get(conf);
    assertEquals(
        ConfVars.RPC_CLIENT_RETRY_NUM.defaultVal, defaultParams.getProperty(RPC_RETRY_NUM));
    assertEquals(
        ConfVars.RPC_CLIENT_CONNECTION_TIMEOUT.defaultVal, defaultParams.getProperty(CLIENT_CONNECTION_TIMEOUT));
    assertEquals(
        ConfVars.RPC_CLIENT_SOCKET_TIMEOUT.defaultVal, defaultParams.getProperty(RpcConstants.CLIENT_SOCKET_TIMEOUT));
  }

  @Test
  public void testGet() throws Exception {
    TajoConf conf = new TajoConf();
    conf.setIntVar(ConfVars.RPC_CLIENT_RETRY_NUM, 100);
    conf.setLongVar(ConfVars.RPC_CLIENT_CONNECTION_TIMEOUT, (long)(10 * 1000));
    conf.setLongVar(ConfVars.RPC_CLIENT_SOCKET_TIMEOUT, (long)60 * 1000);

    Properties defaultParams = RpcConnectionParamUtil.get(conf);
    assertEquals("100", defaultParams.getProperty(RPC_RETRY_NUM));
    assertEquals(10 * 1000 + "", defaultParams.getProperty(CLIENT_CONNECTION_TIMEOUT));
    assertEquals(60 * 1000 + "", defaultParams.getProperty(RpcConstants.CLIENT_SOCKET_TIMEOUT));
  }
}