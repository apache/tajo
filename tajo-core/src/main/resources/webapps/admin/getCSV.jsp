<%
  /*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
%>
<%@ page language = "java" contentType = "charset=utf-8" %>
<%
  response.setHeader("Content-Type", "application/octet-stream;");
  response.setHeader("Content-Disposition", "attachment; filename=\"result.csv\";");
  response.setHeader("Content-Description", "JSP Generated Data");
  response.setHeader("cache-control", "no-cache");
  response.setHeader("expires", "0");
  response.setHeader("pragma", "no-cache");
  out.print(request.getParameter("csvData"));
  out.flush();
%>

