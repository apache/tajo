/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.util.TestProtos.TestMessageProto;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestFileUtils {
	private static final String TEST_PATH = "target/test-data/TestFileUTils";
	TestMessageProto proto = null;	
	
	@Before
	public void setUp() throws Exception {
		TestMessageProto.Builder builder = TestMessageProto.newBuilder();
		builder.setName("TestFileUtils");
		builder.setAge(30);
		builder.setAddr(TestFileUtils.class.getName());
		
		proto = builder.build();
				
		File testDir = new File(TEST_PATH);
		if(testDir.exists()) {
			testDir.delete();
		}
		testDir.mkdirs();
	}
	
	@After
	public void tearDown() throws Exception {
		File testDir = new File(TEST_PATH);
		if(testDir.exists()) {
			testDir.delete();
		}
	}

	@Test
	public final void testWriteLoadProtoFromFile() throws IOException {		
		File file = new File(TEST_PATH+"/file.bin");
		file.createNewFile();
		FileUtil.writeProto(file, proto);
		
		Message defaultInstance = TestMessageProto.getDefaultInstance();
		TestMessageProto message = (TestMessageProto) 
			FileUtil.loadProto(new File(TEST_PATH+"/file.bin"), defaultInstance);
		
		assertEquals(proto, message);
	}

	@Test
	public final void testWriteLoadProtoFromStream() throws IOException {
		FileOutputStream out = new FileOutputStream(new File(TEST_PATH+"/file.bin"));		
		FileUtil.writeProto(out, proto);
		
		
		FileInputStream in = new FileInputStream(new File(TEST_PATH+"/file.bin"));
		Message defaultInstance = TestMessageProto.getDefaultInstance();
		TestMessageProto message = (TestMessageProto) 
			FileUtil.loadProto(in, defaultInstance);
		
		assertEquals(proto, message);
	}

	@Test
	public final void testWriteLoadProtoFromPath() throws IOException {	
		Path path = new Path(TEST_PATH+"/file.bin");
		Configuration conf = new Configuration();
		FileUtil.writeProto(conf, path, proto);
		
		Message defaultInstance = TestMessageProto.getDefaultInstance();
		TestMessageProto message = (TestMessageProto) 
			FileUtil.loadProto(conf, new Path(TEST_PATH+"/file.bin"), defaultInstance);
		
		assertEquals(proto, message);
	}
	
}
