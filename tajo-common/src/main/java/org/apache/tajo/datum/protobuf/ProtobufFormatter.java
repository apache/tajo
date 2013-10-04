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
import com.google.protobuf.UnknownFieldSet;

import java.io.*;
import java.nio.charset.Charset;

public abstract class ProtobufFormatter {
    private Charset defaultCharset = Charset.defaultCharset();

    /**
     * Set the default character set to use for input / output data streams
     * @param cs the character set to use by default, when unspecified
     */
    public void setDefaultCharset(Charset cs) {
        defaultCharset = cs;
    }
    
    /**
     * Get the default character set to use for input / output streams
     * @return the character set to use by default, when unspecified
     */
    public Charset getDefaultCharset() {
        return defaultCharset;
    }
	
	/**
	 * @see print( com.google.protobuf.Message , java.io.OutputStream, java.nio.charset.Charset)
	 * @param message the protobuf message to format
	 * @param output the stream to write the formatted message using the default charset
	 * @throws java.io.IOException
	 */
	public void print(final Message message, OutputStream output) throws IOException {
		print(message, output, defaultCharset);
	}

	/**
	 * Outputs a textual representation of the Protocol Message supplied into
	 * the parameter output. (This representation is the new version of the
	 * classic "ProtocolPrinter" output from the original Protocol Buffer system)
	 *
	 * @param message the protobuf message to format
	 * @param output the stream to write the formatted message
	 * @param cs the character set to use
	 * @throws java.io.IOException
	 */
	abstract public void print(final Message message, OutputStream output, Charset cs) throws IOException;


	/**
	 * @see print( com.google.protobuf.UnknownFieldSet , java.io.OutputStream, java.nio.charset.Charset)
	 * @param fields unknown fields to format
	 * @param output output the stream to write the formatted message using the default charset
	 * @throws java.io.IOException
	 */
	public void print(final UnknownFieldSet fields, OutputStream output) throws IOException {
		print(fields, output, defaultCharset);
	}

	/**
	 * @param fields unknown fields to format
	 * @param output output the stream to write the formatted message
	 * @param cs the character set to use
	 * @throws java.io.IOException
	 */
	abstract public void print(final UnknownFieldSet fields, OutputStream output, Charset cs) throws IOException;
	

	/**
     * Like {@code print()}, but writes directly to a {@code String} and returns it.
     */
	public String printToString(final Message message) {
		try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            print(message, out, defaultCharset);
            out.flush();
            return out.toString();
        } catch (IOException e) {
            throw new RuntimeException("Writing to a StringBuilder threw an IOException (should never happen).",
                                       e);
        }
	}
	
	/**
     * Like {@code print()}, but writes directly to a {@code String} and returns it.
     */
	public String printToString(final UnknownFieldSet fields) {
		try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            print(fields, out, defaultCharset);
            out.flush();
            return out.toString();
        } catch (IOException e) {
            throw new RuntimeException("Writing to a StringBuilder threw an IOException (should never happen).",
                                       e);
        }
	}
	
	/**
     * Thrown when parsing an invalid text format message.
     */
    public static class ParseException extends IOException {
    	private static final long serialVersionUID = 1L;

		public ParseException(String message) {
            super(message);
        }
    }
	
	
	/**
	 * Parse a text-format message from {@code input} and merge the contents
	 * into {@code builder}.
	 */
	abstract public void merge(final InputStream input, Charset cs,
			ExtensionRegistry extensionRegistry,
			final Message.Builder builder) throws IOException;

	
	/**
	 * Parse a text-format message from {@code input} and merge the contents
	 * into {@code builder}.
	 */
	public void merge(final InputStream input, Charset cs, 
			final Message.Builder builder) throws IOException {
		
		merge(input, cs, ExtensionRegistry.getEmptyRegistry(), builder);
	}
	
	public void merge(final InputStream input, 
			final Message.Builder builder) throws IOException {
		
		merge(input, defaultCharset, 
				ExtensionRegistry.getEmptyRegistry(), builder);
	}

  public void merge(final byte [] input, final Message.Builder builder) throws IOException {
    ByteArrayInputStream bio = new ByteArrayInputStream(input);
    merge(bio, defaultCharset, ExtensionRegistry.getEmptyRegistry(), builder);
    bio.close();
  }
	
	public void merge(final InputStream input,
			ExtensionRegistry extensionRegistry,
			final Message.Builder builder) throws IOException {
		merge(input, defaultCharset, extensionRegistry, builder);
	}
}
