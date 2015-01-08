package org.apache.tajo.jdbc.util;

import java.util.List;
import java.util.Map;

import org.apache.tajo.util.TUtil;

public class QueryStringDecoder {

  private final String rawUri;
  private String path;
  private String queries;
  private Map<String, List<String>> params;
  
  public QueryStringDecoder(String rawUri) {
    this.rawUri = rawUri;
  }
  
  private void splitUri() {
    if (rawUri != null && !rawUri.isEmpty()) {
      int pathPos = rawUri.indexOf('?');
      if (pathPos < 0) {
        path = rawUri;
        queries = "";
      } else {
        path = rawUri.substring(0, pathPos);
        if ((pathPos + 1) > rawUri.length()) {
          queries = "";
        } else {
          queries = rawUri.substring(pathPos + 1);
        }
      }
    }
  }
  
  protected void decodeParams() {
    params = TUtil.newHashMap();
    String queries = getQueries();
    
    if (queries != null && !queries.isEmpty()) {
      char c = 0;
      for (int index = 0; index < queries.length(); index++) {
        c = queries.charAt(index);
      }
    }
  }
  
  public String getRawUri() {
    return rawUri;
  }
  
  public String getQueries() {
    if (queries == null || queries.isEmpty()) {
      splitUri();
    }
    return queries;
  }
  
  public Map<String, List<String>> getParameters() {
    if (params == null || params.size() <= 0) {
      decodeParams();
    }
    return params;
  }
}
