package org.apache.tajo.client;

import org.apache.tajo.SessionVars;
import org.apache.tajo.util.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.tajo.SessionVars.COMPRESSED_RESULT_TRANSFER;
import static org.apache.tajo.SessionVars.FETCH_ROWNUM;
import static org.apache.tajo.client.ConnectionParameters.ActionType.CONNECTION_PARAM;
import static org.apache.tajo.client.ConnectionParameters.ActionType.SESSION_UPDATE;
import static org.apache.tajo.rpc.RpcConstants.CLIENT_CONNECTION_TIMEOUT;
import static org.apache.tajo.rpc.RpcConstants.CLIENT_SOCKET_TIMEOUT;

/**
 * <ul>
 *   <li><code>useCompression=bool</code> - Enable compressed transfer for ResultSet. </li>
 *   <li><code>defaultRowFetchSize=int</code> - Determine the number of rows fetched in ResultSet by
 *       one fetch with trip to the Server.</li>
 *   <li><code>connectTimeout=int</code> - The timeout value used for socket connect operations.
 *       If connecting to the server takes longer than this value,the connection is broken. The
 *       timeout is specified in seconds and a value of zero means that it is disabled.</li>
 *   <li><code>socketTimeout=int</code></li> - The timeout value used for socket read operations.
 *       If reading from the server takes longer than this value, the connection is closed.
 *       This can be used as both a brute force global query timeout and a method of detecting
 *       network problems. The timeout is specified in seconds and a value of zero means that
 *       it is disabled.</li>
 * </ul>
 */
public class ConnectionParameters {

  public static Map<String, Action> PARAMETERS = new HashMap<>();

  static {
    PARAMETERS.put("useCompression", new SimpleSessionAction(COMPRESSED_RESULT_TRANSFER));
    PARAMETERS.put("defaultRowFetchSize", new SimpleSessionAction(FETCH_ROWNUM));

    PARAMETERS.put("connectTimeout", new SimpleConnectionParamAction(CLIENT_CONNECTION_TIMEOUT));
    PARAMETERS.put("socketTimeout", new SimpleConnectionParamAction(CLIENT_SOCKET_TIMEOUT));
  }

  enum ActionType {
    SESSION_UPDATE,
    CONNECTION_PARAM
  }

  interface Action {
    ActionType type();
  }

  static class SimpleSessionAction extends SessionAction {
    private final String sessionKey;

    SimpleSessionAction(SessionVars sessionVar) {
      this.sessionKey = sessionVar.name();
    }

    Pair<String, String> doAction(String param) {
      return new Pair<>(sessionKey, param);
    }
  }

  @SuppressWarnings("unused")
  static abstract class SessionAction implements Action {

    @Override
    public ActionType type() {
      return SESSION_UPDATE;
    }

    abstract Pair<String, String> doAction(String param);
  }

  static class SimpleConnectionParamAction extends ConnectionParamAction {
    final String connParamKey;

    SimpleConnectionParamAction(String connParamKey) {
      this.connParamKey = connParamKey;
    }

    public Pair<String, String> doAction(String param) {
      return new Pair<>(connParamKey, param);
    }
  }

  @SuppressWarnings("unused")
  static abstract class ConnectionParamAction implements Action {

    @Override
    public ActionType type() {
      return ActionType.CONNECTION_PARAM;
    }

    abstract Pair<String, String> doAction(String param);
  }

  public static Properties getConnParams(Collection<Map.Entry<String, String>> properties) {
    Properties connParams = new Properties();
    for (Map.Entry<String, String> entry : properties) {
      if(PARAMETERS.containsKey(entry.getKey()) && PARAMETERS.get(entry.getKey()).type() == CONNECTION_PARAM) {
        Pair<String, String> keyValue =
            ((ConnectionParamAction)PARAMETERS.get(entry.getKey())).doAction(entry.getValue());
        connParams.put(keyValue.getFirst(), keyValue.getSecond());
      }
    }

    return connParams;
  }

  public static Map<String, String> getSessionVars(Collection<Map.Entry<String, String>> properties) {
    Map<String, String> sessionVars = new HashMap<>();

    for (Map.Entry<String, String> entry : properties) {
      if(PARAMETERS.containsKey(entry.getKey()) && PARAMETERS.get(entry.getKey()).type() == SESSION_UPDATE) {
        Pair<String, String> keyValue =
            ((SessionAction)PARAMETERS.get(entry.getKey())).doAction(entry.getValue());
        sessionVars.put(keyValue.getFirst(), keyValue.getSecond());
      }
    }

    return sessionVars;
  }
}
