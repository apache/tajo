/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.regex;


import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.InvalidTablePropertyException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.FieldSerializerDeserializer;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.text.TextFieldSerializerDeserializer;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.storage.text.TextLineParsingError;
import org.apache.tajo.storage.text.TextLineSerDe;

import java.io.IOException;
import java.nio.charset.CharsetDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexLineDeserializer extends TextLineDeserializer {
  private static final Log LOG = LogFactory.getLog(RegexLineDeserializer.class);

  private final CharsetDecoder decoder = CharsetUtil.getDecoder(CharsetUtil.UTF_8);
  private FieldSerializerDeserializer fieldSerDer;
  private ByteBuf nullChars;

  private int[] targetColumnIndexes;
  private String inputRegex;
  private Pattern inputPattern;
  // Number of rows not matching the regex
  private long unmatchedRows = 0;
  private long nextUnmatchedRows = 1;
  // Number of rows that match the regex but have missing groups.
  private long partialMatchedRows = 0;
  private long nextPartialMatchedRows = 1;

  public RegexLineDeserializer(Schema schema, TableMeta meta, Column[] projected) {
    super(schema, meta);
    targetColumnIndexes = PlannerUtil.getTargetIds(schema, projected);
  }

  @Override
  public void init() {
    fieldSerDer = new TextFieldSerializerDeserializer(meta);
    fieldSerDer.init(schema);

    // Read the configuration parameters
    inputRegex = meta.getProperty(StorageConstants.TEXT_REGEX);
    boolean inputRegexIgnoreCase = "true".equalsIgnoreCase(
        meta.getProperty(StorageConstants.TEXT_REGEX_CASE_INSENSITIVE, "false"));

    // Parse the configuration parameters
    if (inputRegex != null) {
      inputPattern = Pattern.compile(inputRegex, Pattern.DOTALL
          + (inputRegexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
    } else {
      throw new TajoRuntimeException(new InvalidTablePropertyException(StorageConstants.TEXT_REGEX,
          "This table does not have serde property \"" + StorageConstants.TEXT_REGEX + "\"!"));
    }

    if (nullChars != null) {
      nullChars.release();
    }
    nullChars = TextLineSerDe.getNullChars(meta);
  }


  @Override
  public void deserialize(final ByteBuf lineBuf, Tuple output) throws IOException, TextLineParsingError {

    if (lineBuf == null || targetColumnIndexes.length == 0) {
      return;
    }

    String line = decoder.decode(lineBuf.nioBuffer(lineBuf.readerIndex(), lineBuf.readableBytes())).toString();
    int[] projection = targetColumnIndexes;

    // Projection
    int currentTarget = 0;
    int currentIndex = 0;
    Matcher m = inputPattern.matcher(line);

    if (!m.matches()) {
      unmatchedRows++;
      if (unmatchedRows >= nextUnmatchedRows) {
        nextUnmatchedRows *= 100;
        // Report the row
        LOG.warn("" + unmatchedRows + " unmatched rows are found: " + line);
      }
    } else {

      int groupCount = m.groupCount();
      int currentGroup = 1;
      while (currentGroup <= groupCount) {

        if (projection.length > currentTarget && currentIndex == projection[currentTarget]) {

          try {
            Datum datum = fieldSerDer.deserialize(
                currentIndex, lineBuf.setIndex(m.start(currentGroup), m.end(currentGroup)), nullChars);

            output.put(currentTarget, datum);
          } catch (Exception e) {
            partialMatchedRows++;
            if (partialMatchedRows >= nextPartialMatchedRows) {
              nextPartialMatchedRows *= 100;
              // Report the row
              LOG.warn("" + partialMatchedRows + " partially unmatched rows are found, "
                  + " cannot find group " + currentIndex + ": " + line);
            }
            output.put(currentTarget, NullDatum.get());
          }
          currentTarget++;
        }

        if (projection.length == currentTarget) {
          break;
        }

        currentIndex++;
        currentGroup++;
      }
    }

    /* If a text row is less than table schema size, tuple should set to NullDatum */
    if (projection.length > currentTarget) {
      for (; currentTarget < projection.length; currentTarget++) {
        output.put(currentTarget, NullDatum.get());
      }
    }
  }

  @Override
  public void release() {
    if (nullChars != null) {
      nullChars.release();
      nullChars = null;
    }
  }
}
