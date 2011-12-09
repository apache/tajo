package nta;
import static org.junit.Assert.*;

import nta.conf.NtaConf;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

public class TestLogServer {

	NtaConf conf;
	FileSystem fs;
	Path path;
	FileStatus[] filelist;
	FSDataInputStream in;
	BytesWritable value;
	byte[] reader;
	
	@Before
	public void setup() throws Exception {
		conf = new NtaConf();
		fs = LocalFileSystem.get(conf);
		path = new Path("src/test/resources/");
		filelist = fs.listStatus(path);
	}
	
	@Test
	public void test() throws Exception {
		conf.set(LogServer.LOG_DIR, "src/test/resources/");
		LogServer server = new LogServer(conf);		
		server.init();
		server.start();
		
		BytesWritable logs = server.getLogs();

		in = new FSDataInputStream(fs.open(filelist[0].getPath()));
		reader = new byte[(int)filelist[0].getLen()];
		in.readFully(0, reader);	
		value = new BytesWritable();
		value.set(reader, 0, reader.length);
		in.close();

		assertEquals(logs.getLength(), value.getLength());
		assertEquals(logs, value);
		
		for(int i = 0; i < logs.getLength(); i ++) {
//			if (logs.getBytes()[i] != value.getBytes()[i]) System.out.println(":(");
			assertEquals(logs.getBytes()[i], value.getBytes()[i]);
		}
	}
}
