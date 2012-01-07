/**
 * 
 */
package nta.catalog;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Hyunsik Choi
 *
 */
public class SimpleWAL {
	private final RandomAccessFile raf;
	private final FileChannel fc;
	
	public SimpleWAL(File file) throws IOException {
		raf = new RandomAccessFile(file, "rws");
		fc = raf.getChannel();
	}
	
	public void append(String str) throws IOException {
		str = str+"\n";
		ByteBuffer bb = null;
		try {
			bb = ByteBuffer.wrap(str.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		fc.position(fc.position());
		fc.write(bb);
	}
	
	public void sync() {
		
	}
	
	public void close() throws IOException {
		fc.close();
	}
	
	public static void main(String [] args) throws IOException {
		File file = new File("target/test-data/SimpleWAL/catalog.wal");
		SimpleWAL wal = new SimpleWAL(file);
		wal.append("str\t str\t aaa");
		wal.append("str\t str\t bbb");
		wal.close();
	}
}
