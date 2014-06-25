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
    INVALID,       // Invalid Statement
    STATEMENT_EOS, // End State (End of Statement)
    META_EOS       // End State (End of Statement)
  }

  ParsingState state = START_STATE;
  int lineNum;

  /**
   * It will be used to store a query statement into Jline history.
   * the query statement for history does not include unnecessary white spaces and new line.
   */
  private StringBuilder historyAppender = new StringBuilder();
  /**
   * It will be used to submit a query statement to the TajoMaster. It just contains a raw query statement string.
   */
  private StringBuilder rawAppender = new StringBuilder();

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

    // if parsing continues, it means that the previous line is broken by '\n'.
    // So, we should add new line to rawAppender.
    if (isStatementContinue()) {
      rawAppender.append("\n");
    }

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

          if (isEndOfMeta(character)) {
            state = ParsingState.META_EOS;
          } else if (Character.isWhitespace(character)) {
            // skip
          }
        }

        if (state == ParsingState.META_EOS) {
          historyAppender.append(str.subSequence(lineStartIdx, idx - 1).toString());
          appendToRawStatement(str.subSequence(lineStartIdx, idx - 1).toString(), true);
        } else {
          historyAppender.append(str.subSequence(lineStartIdx, idx).toString());
          appendToRawStatement(str.subSequence(lineStartIdx, idx).toString(), true);
        }

      } else if (isInlineCommentStart(chars, idx)) {
        idx = consumeInlineComment(chars, idx);
        appendToRawStatement(str.subSequence(lineStartIdx, idx).toString(), true);

      /////////////////////////////////
      //    TOK_START     -> STATEMENT
      // or TOK_STATEMENT -> STATEMENT
      ////////////////////////////////
      } else if (isStatementContinue() || isStatementStart(chars[idx])) {
        if (!isStatementContinue()) { // TOK_START -> STATEMENT
          state = ParsingState.STATEMENT;
          rawAppender.append("\n");
        }

        while (!isTerminateState(state) && idx < chars.length) {
          char character = chars[idx++];

          ///////////////////////////////////////////////////////
          // in-statement loop BEGIN
          ///////////////////////////////////////////////////////
          if (isEndOfStatement(character)) {
            state = ParsingState.STATEMENT_EOS;

          } else if (state == ParsingState.STATEMENT && character == '\n') {
            appendToBothStatements(chars, lineStartIdx, idx, 1); // omit new line chacter '\n' from history statement
            lineStartIdx = idx;

          } else if (state == ParsingState.STATEMENT && character == '\'') { // TOK_STATEMENT -> WITHIN_QUOTE
            state = ParsingState.WITHIN_QUOTE;

            if (idx < chars.length) {
              character = chars[idx++];
            } else {
              continue;
            }


            // idx points the characters followed by the current character. So, we should use 'idx - 1'
            // in order to point the current character.
          } else if (state == ParsingState.STATEMENT && idx < chars.length && isInlineCommentStart(chars, idx - 1)) {
            idx++;
            appendToBothStatements(chars, lineStartIdx, idx, 2); // omit two dash characters '--' from history statement
            int commentStartIdx = idx;
            idx = consumeInlineComment(chars, idx);
            appendToRawStatement(str.subSequence(commentStartIdx, idx).toString(), true);
            lineStartIdx = idx;
          }
          ///////////////////////////////////////////////////////
          // in-statement loop END
          ///////////////////////////////////////////////////////

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

        // After all characters are consumed

        if (state == ParsingState.STATEMENT_EOS) { // If one query statement is terminated
          appendToBothStatements(chars, lineStartIdx, idx - 1); // skip semicolon (;)
        } else {
          appendToBothStatements(chars, lineStartIdx, idx);

          // if it is not within quote and there is no space between lines, adds a space.
          if (state == ParsingState.STATEMENT && (historyAppender.charAt(historyAppender.length() - 1) != ' ')) {
            historyAppender.append(" ");
            rawAppender.append("\n");
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

  /**
   * Append the range of characters into a given StringBuilder instance.
   *
   * @param chars Characters
   * @param fromIdx start character index
   * @param toIdx end character index
   */
  private void appendToStatement(StringBuilder builder, char[] chars, int fromIdx, int toIdx) {
    builder.append(chars, fromIdx, toIdx - fromIdx);
  }

  /**
   * Append the range of characters into both history and raw appenders. It omits the number of characters specified by
   * <code>omitCharNums</code>.
   *
   *
   * @param chars Characters
   * @param fromIdx start character index
   * @param toIdx end character index
   * @param omitCharNums how many characters will be omitted from history statement
   */
  private void appendToBothStatements(char[] chars, int fromIdx, int toIdx, int omitCharNums) {
    appendToStatement(historyAppender, chars, fromIdx, toIdx - omitCharNums);
    if (historyAppender.charAt(historyAppender.length() - 1) != ' ') {
      historyAppender.append(" ");
    }
    appendToStatement(rawAppender, chars, fromIdx, toIdx);
  }

  /**
   * Append the range of characters into both history and raw appenders.
   *
   *
   * @param chars Characters
   * @param fromIdx start character index
   * @param toIdx end character index
   */
  private void appendToBothStatements(char[] chars, int fromIdx, int toIdx) {
    historyAppender.append(chars, fromIdx, toIdx - fromIdx);
    rawAppender.append(chars, fromIdx, toIdx - fromIdx);
  }

  private int consumeInlineComment(char [] chars, int currentIdx) {
    currentIdx++;
    while (currentIdx < chars.length && !isNewLine(chars[currentIdx])) {
      currentIdx++;
    }
    return currentIdx;
  }

  private void appendToRawStatement(String str, boolean addLF) {
    if (!str.isEmpty() && !"\n".equals(str) &&
        rawAppender.length() > 0 && addLF && rawAppender.charAt(rawAppender.length() - 1) != '\n') {
      rawAppender.append(str);
    } else {
      rawAppender.append(str);
    }
  }

  private static boolean isEndOfMeta(char character) {
    return character == ';' || character == '\n';
  }

  private static boolean isEndOfStatement(char character) {
    return character == ';';
  }

  /**
   * It checks if inline comment '--' begins.
   * @param chars
   * @param idx
   * @return
   */
  private boolean isInlineCommentStart(char[] chars, int idx) {
    if (idx >= chars.length - 1) {
      return false;
    }
    return (state == ParsingState.STATEMENT || state == ParsingState.TOK_START) &&
        (chars[idx] == '-' && chars[idx + 1] == '-');
  }

  private boolean isNewLine(char character) {
    return character == '\n';
  }

  private boolean isStatementStart(char character) {
    return state == ParsingState.TOK_START && (Character.isLetterOrDigit(character));
  }

  private boolean isStatementContinue() {
    return state == ParsingState.WITHIN_QUOTE || state == ParsingState.STATEMENT;
  }

  /**
   * process all parsed statements so far and return a list of parsed results.
   *
   * @param endOfFile TRUE if the end of file.
   * @return the list of parsed results, each of result contains one query statement or meta command.
   * @throws InvalidStatementException
   */
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
      String historyStatement = historyAppender.toString();
      String rawStatement = rawAppender.toString();
      if (state == ParsingState.META_EOS) {
        parsedResults.add(new ParsedResult(META, rawStatement, historyStatement));
        state = ParsingState.TOK_START;
      } else if (state == ParsingState.STATEMENT_EOS) {
        parsedResults.add(new ParsedResult(STATEMENT, rawStatement, historyStatement));
      } else {
        throw new InvalidStatementException("ERROR: " + errorMessage);
      }

      // reset all states
      historyAppender.delete(0, historyAppender.length());
      rawAppender.delete(0, rawAppender.length());
      state = START_STATE;
    }

    return parsedResults;
  }

  /**
   * It manually triggers the end of file.
   *
   * @return the list of parsed results, each of result contains one query statement or meta command.
   * @throws InvalidStatementException
   */
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
    return "[" + state.name() + "]: " + historyAppender.toString();
  }
}
