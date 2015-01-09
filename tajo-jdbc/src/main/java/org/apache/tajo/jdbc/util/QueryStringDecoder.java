/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.jdbc.util;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryStringDecoder {

  private final Charset charset;
  private final String rawUri;
  private String queries;
  private Map<String, List<String>> params;
  
  public QueryStringDecoder(String rawUri) {
    this(rawUri, Charset.defaultCharset());
  }
  
  public QueryStringDecoder(String rawUri, Charset charset) {
    this.rawUri = rawUri;
    this.charset = charset;
  }
  
  private void splitUri() {
    if (rawUri != null && !rawUri.isEmpty()) {
      int pathPos = rawUri.indexOf('?');
      if (pathPos < 0) {
        queries = "";
      } else {
        if ((pathPos + 1) > rawUri.length()) {
          queries = "";
        } else {
          queries = rawUri.substring(pathPos + 1);
        }
      }
    }
  }
  
  protected void decodeParams() throws MalformedURLException, UnsupportedEncodingException {
    params = new HashMap<String, List<String>>();
    String queries = getQueries();
    
    if (queries != null && !queries.isEmpty()) {
      char c = 0;
      int startPos = 0;
      String name = null, value = null;
      
      for (int index = 0; index < queries.length(); index++) {
        c = queries.charAt(index);
        if (c == '=') {
          name = queries.substring(startPos, index);
          if (name.isEmpty()) {
            throw new MalformedURLException(rawUri + " is not a valid URL.");
          }
          name = decodeString(name);
          startPos = index+1;
        } else if (c == '&') {
          if (name == null || name.isEmpty()) {
            throw new MalformedURLException(rawUri + " is not a valid URL.");
          }          
          value = queries.substring(startPos, index);
          if (value.isEmpty()) {
            throw new MalformedURLException(rawUri + " is not a valid URL.");
          }
          value = decodeString(value);
          addParameter(name, value);
          startPos = index+1;
        }
      }
      
      if (startPos > 0 && name != null && !name.isEmpty()) {
        value = queries.substring(startPos);
        value = decodeString(value);
        addParameter(name, value);
      }
    }
  }
  
  protected String decodeString(String string) throws UnsupportedEncodingException {
    String decoded = "";
    
    if (string != null && !string.isEmpty()) {
      decoded = URLDecoder.decode(string, charset.name());
    }
    
    return decoded;
  }
  
  protected void addParameter(String name, String value) {
    List<String> valueList = params.get(name);
    
    if (valueList == null) {
      valueList = new ArrayList<String>();
      params.put(name, valueList);
    }
    
    valueList.add(value);
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
  
  public Map<String, List<String>> getParameters() throws MalformedURLException, UnsupportedEncodingException {
    if (params == null || params.size() <= 0) {
      decodeParams();
    }
    return params;
  }
}
