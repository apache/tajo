package nta.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import nta.util.TestProtos.TestMessageProto;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Message;

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
	public final void testWriteReadProtoFromFile() throws IOException {		
		File file = new File(TEST_PATH+"/file.bin");
		file.createNewFile();
		FileUtils.writeProto(file, proto);
		
		Message defaultInstance = TestMessageProto.getDefaultInstance();
		TestMessageProto message = (TestMessageProto) 
			FileUtils.loadProto(new File(TEST_PATH+"/file.bin"), defaultInstance);
		
		assertEquals(proto, message);
	}

	@Test
	public final void testWriteReadProtoFromStream() throws IOException {
		FileOutputStream out = new FileOutputStream(new File(TEST_PATH+"/file.bin"));		
		FileUtils.writeProto(out, proto);
		
		
		FileInputStream in = new FileInputStream(new File(TEST_PATH+"/file.bin"));
		Message defaultInstance = TestMessageProto.getDefaultInstance();
		TestMessageProto message = (TestMessageProto) 
			FileUtils.loadProto(in, defaultInstance);
		
		assertEquals(proto, message);
	}
}
