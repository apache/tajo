package nta;

import java.io.IOException;

import nta.conf.NtaConf;
import nta.engine.NtaEngine;
import nta.engine.NtaEngineInterface;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestETL {
	NtaConf conf;
	FileSystem fs;

	@Before
	public void setup() throws IOException, InterruptedException {
		conf = new NtaConf();
		fs = LocalFileSystem.get(conf);
	}
	
	@Test
	public void test() throws IOException, InterruptedException {
		// This comment is a temporal solution during the version 0.3.
		/*
		LogServer server = new LogServer(conf);
		server.init();
		server.start();
		
		server.stop();
		
		NtaEngineInterface engine = new NtaEngine(conf);
		ETL etl = new ETL(conf, engine);
		etl.start();


		Path path = new Path("src/test/resources/test.dat");
		FileStatus file = fs.getFileStatus(path);
		FSDataInputStream in = fs.open(path);
		byte[] b = new byte[(int)file.getLen()];
		in.readFully(b, 0, b.length);
		in.close();
		BytesWritable logs = new BytesWritable(b);
		*/

//		InputStream stdout = lr.flowExport(logs);
//		path = new Path("src/test/resources/test.out");
//		System.out.println("write table");
//		lr.writeTable(path, stdout);
//		
//		
//		
//		in = fs.open(path);
//		BasetableTuple tuple = new BasetableTuple();
//		
//		System.out.println("validation");
//		FileReader fr = new FileReader("src/test/resources/test.csv");
//		BufferedReader br = new BufferedReader(fr);
//		String line, token;
//		StringTokenizer tokenizer;
//
//		try {
//			while (in.available() > 0) {
//				tuple.readFields(in);
//				line = br.readLine();
//				while (line.charAt(0) == '#') {
//					line = br.readLine();
//				}
//				tokenizer = new StringTokenizer(line);
//
//				token = tokenizer.nextToken(",");
//				assertEquals(Integer.valueOf(token), new Integer(tuple.unix_secs));
//				token = tokenizer.nextToken(",");
//				assertEquals(Integer.valueOf(token), new Integer(tuple.unix_nsecs));
//				token = tokenizer.nextToken(",");
//				assertEquals(Integer.valueOf(token), new Integer(tuple.sys_uptime));
//				token = tokenizer.nextToken(",");	// exaddr
//				token = tokenizer.nextToken(",");
//				assertEquals(Integer.valueOf(token), new Integer(tuple.dPkts));
//				token = tokenizer.nextToken(",");
//				assertEquals(Integer.valueOf(token), new Integer(tuple.dOctets));
//				token = tokenizer.nextToken(",");
//				assertEquals(Integer.valueOf(token), new Integer(tuple.first));
//				token = tokenizer.nextToken(",");
//				assertEquals(Integer.valueOf(token), new Integer(tuple.last));
//				token = tokenizer.nextToken(",");	// engine type
//				assertEquals(Byte.valueOf(token), new Byte(tuple.eng_type));
//				token = tokenizer.nextToken(",");
//				assertEquals(Byte.valueOf(token), new Byte(tuple.eng_id));
//				token = tokenizer.nextToken(",");
//				assertEquals(token, new IPv4(tuple.srcAddr).toString());
//				token = tokenizer.nextToken(",");
//				assertEquals(token, new IPv4(tuple.dstAddr).toString());
//				token = tokenizer.nextToken(",");	// nexthop
//				token = tokenizer.nextToken(",");
//				assertEquals(Short.valueOf(token), new Short(tuple.input));
//				token = tokenizer.nextToken(",");
//				assertEquals(Short.valueOf(token), new Short(tuple.output));
//				token = tokenizer.nextToken(",");
//				assertEquals(Short.valueOf(token), new Short(tuple.srcPort));
//				token = tokenizer.nextToken(",");
//				assertEquals(Short.valueOf(token), new Short(tuple.dstPort));
//				token = tokenizer.nextToken(",");
//				assertEquals(Byte.valueOf(token), new Byte(tuple.prot));
//				token = tokenizer.nextToken(",");
//				assertEquals(Byte.valueOf(token), new Byte(tuple.tos));
//				token = tokenizer.nextToken(",");
//				assertEquals(Byte.valueOf(token), new Byte(tuple.tcp_flags));
//				token = tokenizer.nextToken(",");	// src_mask
//				token = tokenizer.nextToken(",");	// dst_mask
//				token = tokenizer.nextToken(",");	// src_as
//				token = tokenizer.nextToken(",");	// dst_as
//			}
//		} catch (EOFException e) {
//			System.out.println("EOFException has occured.");
//		} finally {
//			in.close();
//			assertEquals(br.readLine(), true);
//			br.close();
//			fr.close();
//		}
		
	}
}
