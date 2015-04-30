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

package org.apache.tajo.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.apache.tajo.storage.text.FieldSplitProcessor;
import org.apache.tajo.storage.text.LineSplitProcessor;
import org.apache.tajo.storage.text.MultiBytesFieldSplitProcessor;
import org.junit.Test;

import java.io.IOException;

import static io.netty.util.ReferenceCountUtil.releaseLater;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSplitProcessor {

  @Test
  public void testFieldSplitProcessor() throws IOException {
    String data = "abc||de|";
    final ByteBuf buf = releaseLater(
        Unpooled.copiedBuffer(data, CharsetUtil.ISO_8859_1));

    final int len = buf.readableBytes();
    FieldSplitProcessor processor = new FieldSplitProcessor((byte)'|');

    assertEquals(3, buf.forEachByte(0, len, processor));
    assertEquals(4, buf.forEachByte(4, len - 4, processor));
    assertEquals(7, buf.forEachByte(5, len - 5, processor));
    assertEquals(-1, buf.forEachByte(8, len - 8, processor));
  }

  @Test
  public void testMultiCharFieldSplitProcessor1() throws IOException {
    String data = "abc||||de||";
    final ByteBuf buf = releaseLater(
        Unpooled.copiedBuffer(data, CharsetUtil.ISO_8859_1));

    final int len = buf.readableBytes();
    ByteBufProcessor processor = new MultiBytesFieldSplitProcessor("||".getBytes());

    assertEquals(4, buf.forEachByte(0, len, processor));
    assertEquals(6, buf.forEachByte(5, len - 5, processor));
    assertEquals(10, buf.forEachByte(7, len - 7, processor));
    assertEquals(-1, buf.forEachByte(11, len - 11, processor));
  }

  @Test
  public void testMultiCharFieldSplitProcessor2() throws IOException {
    String data = "abcㅎㅎdeㅎ";
    final ByteBuf buf = releaseLater(
        Unpooled.copiedBuffer(data, CharsetUtil.UTF_8));

    final int len = buf.readableBytes();
    ByteBufProcessor processor = new MultiBytesFieldSplitProcessor("ㅎ".getBytes());

    assertEquals(5, buf.forEachByte(0, len, processor));
    assertEquals(8, buf.forEachByte(6, len - 6, processor));
    assertEquals(13, buf.forEachByte(9, len - 9, processor));
    assertEquals(-1, buf.forEachByte(14, len - 14, processor));
  }

  @Test
  public void testLineSplitProcessor() throws IOException {
    String data = "abc\r\n\n";
    final ByteBuf buf = releaseLater(
        Unpooled.copiedBuffer(data, CharsetUtil.ISO_8859_1));

    final int len = buf.readableBytes();
    LineSplitProcessor processor = new LineSplitProcessor();

    //find CR
    assertEquals(3, buf.forEachByte(0, len, processor));

    // find CRLF
    assertEquals(4, buf.forEachByte(4, len - 4, processor));
    assertEquals(buf.getByte(4), '\n');
    // need to skip LF
    assertTrue(processor.isPrevCharCR());

    // find LF
    assertEquals(5, buf.forEachByte(5, len - 5, processor)); //line length is zero
  }
}
