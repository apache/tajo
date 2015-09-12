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

package org.apache.tajo.parser.sql;


import org.antlr.v4.runtime.Token;
import org.apache.commons.lang.StringUtils;

/**
 * Exception that represents a kind of SQL syntax error caused by the parser layer
 */
public class SQLParseError extends RuntimeException {
  private String header;
  private String errorLine;
  private int charPositionInLine;
  private int line;
  private Token offendingToken;
  private String detailedMessage;

  public SQLParseError(Token offendingToken,
                       int line, int charPositionInLine,
                       String msg,
                       String errorLine) {
    super(msg);
    this.offendingToken = offendingToken;
    this.charPositionInLine = charPositionInLine;
    this.line = line;
    this.errorLine = errorLine;
    this.header = msg;
  }

  @Override
  public String getMessage() {
    if (detailedMessage == null) {
      if (offendingToken != null) {
        detailedMessage = getDetailedMessageWithLocation();
      } else {
        StringBuilder sb = new StringBuilder();
        sb.append("ERROR: ").append(header).append("\n");
        sb.append("LINE: ").append(errorLine);
        detailedMessage = sb.toString();
      }
    }

    return detailedMessage;
  }

  private String getDetailedMessageWithLocation() {
    StringBuilder sb = new StringBuilder();
    int displayLimit = 80;
    String queryPrefix = "LINE " + line + ":" + " ";
    String prefixPadding = StringUtils.repeat(" ", queryPrefix.length());
    String locationString;

    int tokenLength = offendingToken.getStopIndex() - offendingToken.getStartIndex() + 1;
    if(tokenLength > 0){
      locationString = StringUtils.repeat(" ", charPositionInLine) + StringUtils.repeat("^", tokenLength);
    } else {
      locationString = StringUtils.repeat(" ", charPositionInLine) + "^";
    }

    sb.append("ERROR: ").append(header).append("\n");
    sb.append(queryPrefix);

    if (errorLine.length() > displayLimit) {
      int padding = (displayLimit / 2);

      String ellipsis = " ... ";
      int startPos = locationString.length() - padding - 1;
      if (startPos <= 0) {
        startPos = 0;
        sb.append(errorLine.substring(startPos, displayLimit)).append(ellipsis).append("\n");
        sb.append(prefixPadding).append(locationString);
      } else if (errorLine.length() - (locationString.length() + padding) <= 0) {
        startPos = errorLine.length() - displayLimit - 1;
        sb.append(ellipsis).append(errorLine.substring(startPos)).append("\n");
        sb.append(prefixPadding).append(StringUtils.repeat(" ", ellipsis.length()))
            .append(locationString.substring(startPos));
      } else {
        sb.append(ellipsis).append(errorLine.substring(startPos, startPos + displayLimit)).append(ellipsis).append("\n");
        sb.append(prefixPadding).append(StringUtils.repeat(" ", ellipsis.length()))
            .append(locationString.substring(startPos));
      }
    } else {
      sb.append(errorLine).append("\n");
      sb.append(prefixPadding).append(locationString);
    }
    return sb.toString();
  }
}
