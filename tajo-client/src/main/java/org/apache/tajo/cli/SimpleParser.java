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

package org.apache.tajo.cli;

import java.util.ArrayList;
import java.util.List;

import static org.apache.tajo.cli.ParsedResult.StatementType.META;
import static org.apache.tajo.cli.ParsedResult.StatementType.STATEMENT;

/**
 * This is a parser used in tsql to parse multiple SQL lines into SQL statements.
 * It helps tsql recognizes the termination of each SQL statement and quotation mark (') while
 * parses multiple separate lines.
 */
public class SimpleParser {

  public static enum ParsingState {
    TOK_START,     // Start State
    META,          // Meta Command
    STATEMENT,     // Statement
    WITHIN_QUOTE,  // Within Quote
    COMMENT,
    INVALID,       // Invalid Statement
    STATEMENT_EOS, // End State (End of Statement)
    META_EOS       // End State (End of Statement)
  }

  ParsingState state = START_STATE;
  int lineNum;
  StringBuilder appender = new StringBuilder();

  public static final ParsingState START_STATE = ParsingState.TOK_START;

  /**
   * <h2>State Machine</h2>
   * All whitespace are ignored in all cases except for
   *
   * <pre>
   * (start) TOK_START --> META ---------------------> META_EOS
   *                    |
   *                    |
   *                    |
   *                    |-----------> STATEMENT ----------> STMT_EOS
   *                                  \       ^
   *                                  \      /
   *                                  \-> WITHIN_QUOTE
   *                                        \    ^
   *                                        \---/
   * </pre>
   */

  public static List<ParsedResult> parseScript(String str) throws InvalidStatementException {
    SimpleParser parser = new SimpleParser();
    List<ParsedResult> parsedResults = new ArrayList<ParsedResult>();
    parsedResults.addAll(parser.parseLines(str));
    parsedResults.addAll(parser.EOF());
    return parsedResults;
  }

  public List<ParsedResult> parseLines(String str) throws InvalidStatementException {
    List<ParsedResult> statements = new ArrayList<ParsedResult>();
    int lineStartIdx;
    int idx = 0;
    char [] chars = str.toCharArray();

    while(idx < str.length()) {

      // initialization for new statement
      if (state == ParsingState.TOK_START) {
        lineNum = 0;

        // ignore all whitespace before start
        if (Character.isWhitespace(chars[idx])) {
          idx++;
          continue;
        }
      }

      ////////////////////////////
      // TOK_START --> META
      ////////////////////////////

      lineStartIdx = idx;

      if (state == ParsingState.TOK_START && chars[idx] == '\\') {
        state = ParsingState.META;

        ////////////////////////////
        // META --> TOK_EOS
        ////////////////////////////
        while (state != ParsingState.META_EOS && idx < chars.length) {
          char character = chars[idx++];
          if (Character.isWhitespace(character)) {
            // skip
          } else if (isEndOfMeta(character)) {
            state = ParsingState.META_EOS;
          }
        }

        if (state == ParsingState.META_EOS) {
          appender.append(str.subSequence(lineStartIdx, idx - 1).toString());
        } else {
          appender.append(str.subSequence(lineStartIdx, idx).toString());
        }

      } else if (isCommentStart(chars[idx])) {
        idx++;
        while (!isLineEnd(chars[idx]) && idx < chars.length) {
          idx++;
        }
      /////////////////////////////////
      //    TOK_START     -> STATEMENT
      // or TOK_STATEMENT -> STATEMENT
      ////////////////////////////////
      } else if (isStatementContinue() || isStatementStart(chars[idx])) {
        int endIdx = 0;
        if (!isStatementContinue()) { // TOK_START -> STATEMENT
          state = ParsingState.STATEMENT;
        }

        while (!isTerminateState(state) && idx < chars.length) {
          char character = chars[idx++];

          if (isEndOfStatement(character)) {
            state = ParsingState.STATEMENT_EOS;
            endIdx = idx - 1;
          } else if (state == ParsingState.STATEMENT && character == '\'') { // TOK_STATEMENT -> WITHIN_QUOTE
            state = ParsingState.WITHIN_QUOTE;

            if (idx < chars.length) {
              character = chars[idx++];
            } else {
              continue;
            }
          }

          if (state == ParsingState.WITHIN_QUOTE) {
            while(idx < chars.length) {
              ///////////////////////////////
              // WITHIN_QUOTE --> STATEMENT
              ///////////////////////////////
              if (character == '\'') {
                state = ParsingState.STATEMENT;
                break;
              }
              character = chars[idx++];
            }
            if (state == ParsingState.WITHIN_QUOTE && character == '\'') {
              state = ParsingState.STATEMENT;
            }
          }
        }

        if (state == ParsingState.STATEMENT_EOS) {
          appender.append(str.subSequence(lineStartIdx, endIdx).toString());
        } else {
          appender.append(str.subSequence(lineStartIdx, idx).toString());

          // if it is not within quote and there is no space between lines, add a space.
          if (state == ParsingState.STATEMENT && (appender.charAt(appender.length() - 1) != ' ')) {
            appender.append(" ");
          }
        }
      } else { // skip unknown character
        idx++;
      }

      lineNum++;
      statements.addAll(doProcessEndOfStatement(state == ParsingState.META));
    }

    return statements;
  }

  private static boolean isEndOfMeta(char character) {
    return character == ';' || character == '\n';
  }

  private static boolean isEndOfStatement(char character) {
    return character == ';';
  }

  private boolean isCommentStart(char character) {
    return state == ParsingState.TOK_START && character == '-';
  }

  private boolean isLineEnd(char character) {
    return character == '\n';
  }

  private boolean isStatementStart(char character) {
    return state == ParsingState.TOK_START && (Character.isLetterOrDigit(character));
  }

  private boolean isStatementContinue() {
    return state == ParsingState.WITHIN_QUOTE || state == ParsingState.STATEMENT;
  }

  private List<ParsedResult> doProcessEndOfStatement(boolean endOfFile) throws InvalidStatementException {
    List<ParsedResult> parsedResults = new ArrayList<ParsedResult>();
    String errorMessage = "";
    if (endOfFile) {
      if (state == ParsingState.META) {
        state = ParsingState.META_EOS;
      } else if (state == ParsingState.STATEMENT) {
        state = ParsingState.STATEMENT_EOS;
      } else if (state == ParsingState.WITHIN_QUOTE) {
        state = ParsingState.INVALID;
        errorMessage = "unterminated quoted string at LINE " + lineNum;
      }
    }

    if (isTerminateState(state)) {
      String statement = appender.toString();
      if (state == ParsingState.META_EOS) {
        parsedResults.add(new ParsedResult(META, statement));
        state = ParsingState.TOK_START;
      } else if (state == ParsingState.STATEMENT_EOS) {
        parsedResults.add(new ParsedResult(STATEMENT, statement));
      } else {
        throw new InvalidStatementException("ERROR: " + errorMessage);
      }

      // reset all states
      appender.delete(0, appender.length());
      state = START_STATE;
    }

    return parsedResults;
  }

  public List<ParsedResult> EOF() throws InvalidStatementException {
    return doProcessEndOfStatement(true);
  }

  private static boolean isTerminateState(ParsingState state) {
    return (state == ParsingState.META_EOS || state == ParsingState.STATEMENT_EOS || state == ParsingState.INVALID);
  }

  public ParsingState getState() {
    return state;
  }

  public String toString() {
    return "[" + state.name() + "]: " + appender.toString();
  }
}
