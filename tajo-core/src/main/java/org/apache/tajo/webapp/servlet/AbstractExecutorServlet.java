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

package org.apache.tajo.webapp.servlet;

import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractExecutorServlet extends HttpServlet {
  protected transient ObjectMapper om = new ObjectMapper();

  protected void writeObject(java.io.ObjectOutputStream stream) throws java.io.IOException {
    throw new NotSerializableException( getClass().getName() );
  }

  protected void readObject(java.io.ObjectInputStream stream) throws java.io.IOException, ClassNotFoundException {
    throw new NotSerializableException( getClass().getName() );
  }

  protected void errorResponse(HttpServletResponse response, Exception e) throws IOException {
    errorResponse(response, e, null);
  }

  protected void errorResponse(HttpServletResponse response, Exception e, String type) throws IOException {
    errorResponse(response, e.getMessage() + "\n" + StringUtils.stringifyException(e), type);
  }

  protected void errorResponse(HttpServletResponse response, String message) throws IOException {
    errorResponse(response, message, null);
  }

  protected void errorResponse(HttpServletResponse response, String message, String type) throws IOException {
    Map<String, Object> errorMessage = new HashMap<String, Object>();
    errorMessage.put("success", "false");
    errorMessage.put("errorMessage", message);
    errorMessage.put("timestamp", Calendar.getInstance().getTimeInMillis());
    writeHttpResponse(response, errorMessage, type);
  }

  protected void writeHttpResponse(HttpServletResponse response, Map<String, Object> outputMessage) throws IOException {
    writeHttpResponse(response, outputMessage, null);
  }

  protected void writeHttpResponse(HttpServletResponse response, Map<String, Object> outputMessage, String type) throws IOException {
    if(type==null){
      type = "text/html";
    }
    response.setContentType(type);

    OutputStream out = response.getOutputStream();
    out.write(om.writeValueAsBytes(outputMessage));

    out.flush();
    out.close();
  }
}
