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
import org.antlr.v4.runtime.misc.NotNull;

public class SQLErrorStrategy extends DefaultErrorStrategy {

  @Override
  public void reportError(Parser recognizer, RecognitionException e) {
    // if we've already reported an error and have not matched a token
    // yet successfully, don't report any errors.
    if (inErrorRecoveryMode(recognizer)) {
      return; // don't report spurious errors
    }
    beginErrorCondition(recognizer);
    if (e instanceof NoViableAltException) {
      reportNoViableAltException(recognizer, (NoViableAltException) e);
    } else if (e instanceof InputMismatchException) {
      reportInputMismatchException(recognizer, (InputMismatchException) e);
    } else if (e instanceof FailedPredicateException) {
      reportFailedPredicate(recognizer, (FailedPredicateException) e);
    } else {
      recognizer.notifyErrorListeners(e.getOffendingToken(), e.getMessage(), e);
    }
  }

  protected void reportNoViableAltException(@NotNull Parser recognizer, @NotNull NoViableAltException e) {
    TokenStream tokens = recognizer.getInputStream();
    String msg;
    Token token = e.getStartToken();
    if (tokens != null) {
      if (tokens.LT(-1) != null && token.getType() == Token.EOF) {
        token = tokens.LT(-1);
      }
      msg = "syntax error at or near " + getTokenErrorDisplay(token);
    } else {
      msg = "no viable alternative at input " + escapeWSAndQuote("<unknown input>");
    }
    recognizer.notifyErrorListeners(token, msg, e);
  }

  protected void reportInputMismatchException(@NotNull Parser recognizer,
                                              @NotNull InputMismatchException e) {
    String msg = "mismatched input " + getTokenErrorDisplay(e.getOffendingToken()) +
        " expecting " + e.getExpectedTokens().toString(recognizer.getTokenNames());
    recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
  }
}
