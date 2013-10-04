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

package org.apache.tajo.datum.protobuf;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.UnknownFieldSet;

import java.io.*;
import java.nio.charset.Charset;

public abstract class AbstractCharBasedFormatter extends ProtobufFormatter {

	@Override
	public void print(Message message, OutputStream output, Charset cs)
			throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(output, cs);
		print(message, writer);
		writer.flush();
	}
	
	abstract public void print(Message message, Appendable output) throws IOException;


	@Override
	public void print(UnknownFieldSet fields, OutputStream output, Charset cs)
			throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(output, cs);
		print(fields, writer);
		writer.flush();
	}
	
	abstract public void print(UnknownFieldSet fields, Appendable output) throws IOException;

	@Override
	public void merge(InputStream input, Charset cs, 
			ExtensionRegistry extensionRegistry, Builder builder) throws IOException {
		InputStreamReader reader = new InputStreamReader(input, cs);
		merge(reader, extensionRegistry, builder);
	}
	
	
	abstract public void merge(CharSequence input, ExtensionRegistry extensionRegistry,
            Message.Builder builder) throws IOException;

	/**
     * Parse a text-format message from {@code input} and merge the contents into {@code builder}.
     * Extensions will be recognized if they are registered in {@code extensionRegistry}.
     */
    public void merge(Readable input,
    		ExtensionRegistry extensionRegistry,
    		Message.Builder builder) throws IOException {
        // Read the entire input to a String then parse that.

        // If StreamTokenizer were not quite so crippled, or if there were a kind
        // of Reader that could read in chunks that match some particular regex,
        // or if we wanted to write a custom Reader to tokenize our stream, then
        // we would not have to read to one big String. Alas, none of these is
        // the case. Oh well.

		merge(TextUtils.toStringBuilder(input), extensionRegistry, builder);
    }
}
