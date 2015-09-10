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

import org.antlr.v4.runtime.*;
import org.apache.commons.lang.StringUtils;
import org.apache.tajo.exception.TajoRuntimeException;

public class SQLErrorListener extends BaseErrorListener {

  public void syntaxError(Recognizer<?, ?> recognizer,
                          Object offendingSymbol,
                          int line, int charPositionInLine,
                          String msg,
                          RecognitionException e) {
    CommonTokenStream tokens = (CommonTokenStream) recognizer.getInputStream();
    String input = tokens.getTokenSource().getInputStream().toString();
    Token token = (Token) offendingSymbol;
    String[] lines = StringUtils.splitPreserveAllTokens(input, '\n');
    String errorLine = lines[line - 1];

    String simpleMessage = "syntax error at or near \"" + token.getText() + "\"";
    throw new TajoRuntimeException(new SQLParseError(token, line, charPositionInLine, simpleMessage, errorLine));
  }
}
