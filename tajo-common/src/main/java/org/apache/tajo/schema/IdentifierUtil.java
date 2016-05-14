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

package org.apache.tajo.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.schema.IdentifierPolicy.IdentifierCase;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.StringUtils;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.apache.tajo.schema.Identifier._;
import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;
import static org.apache.tajo.schema.QualifiedIdentifier.$;

/**
 * Util methods for Identifiers
 */
public class IdentifierUtil {

  public final static String IDENTIFIER_DELIMITER_REGEXP = "\\.";
  public final static String IDENTIFIER_DELIMITER = ".";
  public final static String IDENTIFIER_QUOTE_STRING = "\"";
  public final static int MAX_IDENTIFIER_LENGTH = 128;

  public static final Set<String> RESERVED_KEYWORDS_SET = new HashSet<>();
  public static final String [] RESERVED_KEYWORDS = {
      "AS", "ALL", "AND", "ANY", "ASYMMETRIC", "ASC",
      "BOTH",
      "CASE", "CAST", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
      "DESC", "DISTINCT",
      "END", "ELSE", "EXCEPT",
      "FALSE", "FULL", "FROM",
      "GROUP",
      "HAVING",
      "ILIKE", "IN", "INNER", "INTERSECT", "INTO", "IS",
      "JOIN",
      "LEADING", "LEFT", "LIKE", "LIMIT",
      "NATURAL", "NOT", "NULL",
      "ON", "OUTER", "OR", "ORDER",
      "RIGHT",
      "SELECT", "SOME", "SYMMETRIC",
      "TABLE", "THEN", "TRAILING", "TRUE",
      "OVER",
      "UNION", "UNIQUE", "USING",
      "WHEN", "WHERE", "WINDOW", "WITH"
  };

  public static QualifiedIdentifier makeIdentifier(String raw, IdentifierPolicy policy) {
    if (raw == null || raw.equals("")) {
      return $(raw);
    }

    final String [] splitted = raw.split(IDENTIFIER_DELIMITER_REGEXP);
    final ImmutableList.Builder<Identifier> builder = ImmutableList.builder();

    for (String part : splitted) {
      boolean quoted = isDelimited(part, policy);
      if (quoted) {
        builder.add(_(stripQuote(part), true));
      } else {
        builder.add(internIdentifierPart(part, policy));
      }
    }
    return $(builder.build());
  }

  public static Identifier internIdentifierPart(String rawPart, IdentifierPolicy policy) {
    IdentifierCase unquotedIdentifierCase = policy.storesUnquotedIdentifierAs();
    final String interned;
    if (unquotedIdentifierCase == IdentifierCase.LowerCase) {
      interned = rawPart.toLowerCase(Locale.ENGLISH);
    } else if (unquotedIdentifierCase == IdentifierCase.UpperCase) {
      interned = rawPart.toUpperCase(Locale.ENGLISH);
    } else {
      interned = rawPart;
    }

    return _(interned, false);
  }

  /**
   * Normalize an identifier. Normalization means a translation from a identifier to be a refined identifier name.
   *
   * Identifier can be composed of multiple parts as follows:
   * <pre>
   *   database_name.table_name.column_name
   * </pre>
   *
   * Each regular identifier part can be composed alphabet ([a-z][A-Z]), number([0-9]), and underscore([_]).
   * Also, the first letter must be an alphabet character.
   *
   * <code>normalizeIdentifier</code> normalizes each part of an identifier.
   *
   * In detail, for each part, it performs as follows:
   * <ul>
   *   <li>changing a part without double quotation to be lower case letters</li>
   *   <li>eliminating double quotation marks from identifier</li>
   * </ul>
   *
   * @param identifier The identifier to be normalized
   * @return The normalized identifier
   */
  public static String normalizeIdentifier(String identifier) {
    if (identifier == null || identifier.equals("")) {
      return identifier;
    }
    String [] splitted = identifier.split(IDENTIFIER_DELIMITER_REGEXP);

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String part : splitted) {
      if (first) {
        first = false;
      } else {
        sb.append(IDENTIFIER_DELIMITER);
      }
      sb.append(normalizeIdentifierPart(part));
    }
    return sb.toString();
  }

  public static String normalizeIdentifierPart(String part) {
    return isDelimited(part) ? stripQuote(part) : part.toLowerCase();
  }

  /**
   * Denormalize an identifier. Denormalize means a translation from a stored identifier
   * to be a printable identifier name.
   *
   * In detail, for each part, it performs as follows:
   * <ul>
   *   <li>changing a part including upper case character or non-ascii character to be lower case letters</li>
   *   <li>eliminating double quotation marks from identifier</li>
   * </ul>
   *
   * @param identifier The identifier to be normalized
   * @return The denormalized identifier
   */
  public static String denormalizeIdentifier(String identifier) {
    String [] splitted = identifier.split(IDENTIFIER_DELIMITER_REGEXP);

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String part : splitted) {
      if (first) {
        first = false;
      } else {
        sb.append(IDENTIFIER_DELIMITER);
      }
      sb.append(denormalizePart(part));
    }
    return sb.toString();
  }

  public static String denormalizePart(String identifier) {
    if (isShouldBeQuoted(identifier)) {
      return StringUtils.doubleQuote(identifier);
    } else {
      return identifier;
    }
  }

  public static boolean isShouldBeQuoted(String interned) {
    for (char character : interned.toCharArray()) {
      if (Character.isUpperCase(character)) {
        return true;
      }

      if (!StringUtils.isPartOfAnsiSQLIdentifier(character)) {
        return true;
      }

      if (RESERVED_KEYWORDS_SET.contains(interned.toUpperCase())) {
        return true;
      }
    }

    return false;
  }

  public static String stripQuote(String raw) {
    return raw.substring(1, raw.length() - 1);
  }

  public static boolean isDelimited(String raw) {
    return isDelimited(raw, DefaultPolicy());
  }

  public static boolean isDelimited(String raw, IdentifierPolicy policy) {
    char quoteString = policy.getIdentifierQuoteString();
    boolean openQuote = raw.charAt(0) == quoteString;
    boolean closeQuote = raw.charAt(raw.length() - 1) == quoteString;

    // if at least one quote mark exists, the identifier must be grater than equal to 2 characters,
    if (openQuote ^ closeQuote && raw.length() < 2) {
      throw new IllegalArgumentException("Invalid Identifier: " + raw);
    }

    // does not allow the empty identifier (''),
    if (openQuote && closeQuote && raw.length() == 2) {
      throw new IllegalArgumentException("zero-length delimited identifier: " + raw);
    }

    // Ensure the quote open and close
    return openQuote && closeQuote;
  }

  /**
   * True if a given name is a simple identifier, meaning is not a dot-chained name.
   *
   * @param columnOrTableName Column or Table name to be checked
   * @return True if a given name is a simple identifier. Otherwise, it will return False.
   */
  public static boolean isSimpleIdentifier(String columnOrTableName) {
    return columnOrTableName.split(IDENTIFIER_DELIMITER_REGEXP).length == 1;
  }

  public static boolean isFQColumnName(String tableName) {
    return tableName.split(IDENTIFIER_DELIMITER_REGEXP).length == 3;
  }

  public static boolean isFQTableName(String tableName) {
    int lastDelimiterIdx = tableName.lastIndexOf(IDENTIFIER_DELIMITER);
    return lastDelimiterIdx > -1;
  }

  public static String [] splitFQTableName(String qualifiedName) {
    String [] splitted = splitTableName(qualifiedName);
    if (splitted.length == 1) {
      throw new IllegalArgumentException("Table name is expected to be qualified, but was \""
          + qualifiedName + "\".");
    }
    return splitted;
  }

  public static String [] splitTableName(String tableName) {
    int lastDelimiterIdx = tableName.lastIndexOf(IDENTIFIER_DELIMITER);
    if (lastDelimiterIdx > -1) {
      return new String [] {
          tableName.substring(0, lastDelimiterIdx),
          tableName.substring(lastDelimiterIdx + 1, tableName.length())
      };
    } else {
      return new String [] {tableName};
    }
  }

  public static String buildFQName(String... identifiers) {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for(String id : identifiers) {
      if (first) {
        first = false;
      } else {
        sb.append(IDENTIFIER_DELIMITER);
      }

      sb.append(id);
    }

    return sb.toString();
  }

  public static Pair<String, String> separateQualifierAndName(String name) {
    Preconditions.checkArgument(isFQTableName(name), "Must be a qualified name.");
    return new Pair<>(extractQualifier(name), extractSimpleName(name));
  }

  /**
   * Extract a qualification name from an identifier.
   *
   * For example, consider a table identifier like 'database1.table1'.
   * In this case, this method extracts 'database1'.
   *
   * @param name The identifier to be extracted
   * @return The extracted qualifier
   */
  public static String extractQualifier(String name) {
    int lastDelimiterIdx = name.lastIndexOf(IDENTIFIER_DELIMITER);
    if (lastDelimiterIdx > -1) {
      return name.substring(0, lastDelimiterIdx);
    } else {
      return TajoConstants.EMPTY_STRING;
    }
  }

  /**
   * Extract a simple name from an identifier.
   *
   * For example, consider a table identifier like 'database1.table1'.
   * In this case, this method extracts 'table1'.
   *
   * @param name The identifier to be extracted
   * @return The extracted simple name
   */
  public static String extractSimpleName(String name) {
    int lastDelimiterIdx = name.lastIndexOf(IDENTIFIER_DELIMITER);
    if (lastDelimiterIdx > -1) {
      // plus one means skipping a delimiter.
      return name.substring(lastDelimiterIdx + 1, name.length());
    } else {
      return name;
    }
  }

  public static String getCanonicalTableName(String databaseName, String tableName) {
    StringBuilder sb = new StringBuilder(databaseName);
    sb.append(IDENTIFIER_DELIMITER);
    sb.append(tableName);
    return sb.toString();
  }
}
