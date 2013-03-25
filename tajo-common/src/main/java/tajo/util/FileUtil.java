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

package tajo.util;

import com.google.protobuf.Message;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FileUtil {
	public static void writeProto(File file, Message proto) throws IOException {
		FileOutputStream stream = new FileOutputStream(file);
		stream.write(proto.toByteArray());
		stream.close();		
	}
	public static void writeProto(OutputStream out, Message proto) throws IOException {
		out.write(proto.toByteArray());
	}
	
	public static void writeProto(FileSystem fs, Path path, Message proto) throws IOException {
		FSDataOutputStream stream = fs.create(path);
		stream.write(proto.toByteArray());
		stream.close();
	}
	
	public static Message loadProto(File file, Message proto) throws IOException {
		FileInputStream in = new FileInputStream(file);
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);		
		return builder.build();
	}
	
	public static Message loadProto(InputStream in, Message proto) throws IOException {
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
		return builder.build();
	}
	
	public static Message loadProto(FileSystem fs,
                                  Path path, Message proto) throws IOException {
		FSDataInputStream in = new FSDataInputStream(fs.open(path));
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
		return builder.build();
	}
	
	public static File getFile(String path) {
		return new File(path);
	}
	
	public static String readTextFile(File file) throws IOException {
    StringBuilder fileData = new StringBuilder(1000);
    BufferedReader reader = new BufferedReader(
            new FileReader(file));
    char[] buf = new char[1024];
    int numRead;
    while((numRead=reader.read(buf)) != -1){
        String readData = String.valueOf(buf, 0, numRead);
        fileData.append(readData);
        buf = new char[1024];
    }
    reader.close();
    return fileData.toString();
  }

  public static String humanReadableByteCount(long bytes, boolean si) {
    int unit = si ? 1000 : 1024;
    if (bytes < unit) return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }

  public static boolean isLocalPath(Path path) {
    return path.toUri().getScheme().equals("file");
  }
}
