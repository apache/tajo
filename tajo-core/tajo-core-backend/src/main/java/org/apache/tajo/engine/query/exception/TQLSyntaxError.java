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

package org.apache.tajo.engine.query.exception;


import org.apache.commons.lang.StringUtils;

public class TQLSyntaxError extends InvalidQueryException {
  private static final long serialVersionUID = 5388279335175632066L;

  private String query;
  private String errorMessage;
  private String detailedMessage;
  private TQLParseError parseError;

  public TQLSyntaxError(String query, String errorMessage) {
    this.query = query;
    this.errorMessage = errorMessage;
  }

  public TQLSyntaxError(String query, TQLParseError e) {
    this.query = query;
    this.errorMessage = e.getMessage();
    this.parseError = e;
  }

  @Override
  public String getMessage() {
    if (detailedMessage == null) {
      if (parseError != null) {
        detailedMessage = getDetailedMessageWithLocation();
      } else {
        StringBuilder sb = new StringBuilder();
        sb.append("ERROR: " + errorMessage).append("\n");
        sb.append("LINE: " + query);
        detailedMessage = sb.toString();
      }
    }

    return detailedMessage;
  }

  private String getDetailedMessageWithLocation() {
    StringBuilder sb = new StringBuilder();
    int displayLimit = 80;
    String queryPrefix = "LINE " + parseError.getErrorLine() + ":" +parseError.getErrorPosition() + " ";
    String prefixPadding = StringUtils.repeat(" ", queryPrefix.length());
    String locationString = StringUtils.repeat(" ", parseError.getErrorPosition()) + "^";

    sb.append("ERROR: " + this.errorMessage).append("\n");
    sb.append(queryPrefix);

    if (query.length() > displayLimit) {
      int padding = (displayLimit / 2);

      String ellipsis = " ... ";
      int startPos = locationString.length() - padding - 1;
      if (startPos <= 0) {
        startPos = 0;
        sb.append(query.substring(startPos, displayLimit)).append(ellipsis).append("\n");
        sb.append(prefixPadding).append(locationString);
      } else if (query.length() - (locationString.length() + padding) <= 0) {
        startPos = query.length() - displayLimit - 1;
        sb.append(ellipsis).append(query.substring(startPos)).append("\n");
        sb.append(prefixPadding).append(locationString.substring(startPos - ellipsis.length()));
      } else {
        sb.append(ellipsis).append(query.substring(startPos, startPos + displayLimit)).append(ellipsis).append("\n");
        sb.append(prefixPadding).append(locationString.substring(startPos - ellipsis.length()));
      }
    } else {
      sb.append(query).append("\n");
      sb.append(prefixPadding).append(locationString);
    }
    return sb.toString();
  }
}
