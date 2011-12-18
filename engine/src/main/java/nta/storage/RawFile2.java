package nta.storage;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.storage.exception.ReadOnlyException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RawFile2 {
	
	public static final Log LOG = LogFactory.getLog(RawFile2.class);
	
	private RawFile2() {
		
	}
	
	public static RawFileScanner getScanner(NtaConf conf, Store store, long start, long end) throws IOException {
		return new RawFileScanner(conf, store, start, end);
	}
	
	public static RawFileAppender getAppender(NtaConf conf, Store store) throws IOException {
		return new RawFileAppender(conf, store); 
	}

	private static final int SYNC_ESCAPE = -1;
	private static final int SYNC_HASH_SIZE = 16;
	private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE;
	public static int SYNC_INTERVAL;
	
	public static class RawFileScanner implements FileScanner {
		
		private NtaConf conf;
		private Schema schema;
		private Path path;
		private FSDataInputStream in;
		private FileSystem fs;
		private byte[] sync;
		private byte[] checkSync;
		private int escape;
		private long start, end;
		private long lastSyncPos;
		private long headPos;
		
		public RawFileScanner(NtaConf conf, Store store, long start, long end) throws IOException {
			this.conf = conf;
			this.schema = store.getSchema();
			this.path = new Path(new Path(store.getURI()), "data/table.raw");
			this.fs = path.getFileSystem(conf);
			this.start = start;
			FileStatus status = fs.getFileStatus(path);
			this.end = end > status.getLen() ? status.getLen() : end;

			in = fs.open(path);
			
			sync = new byte[SYNC_HASH_SIZE];
			checkSync = new byte[SYNC_HASH_SIZE];
			
			init();
		}
		
		private void init() throws IOException {
			readHeader();
			headPos = in.getPos();
			if (start < headPos) {
				in.seek(headPos);
			} else {
				in.seek(start);
			}
			if (in.getPos() != lastSyncPos) {
				for (int i = 0; in.getPos() < end; i++) {
					if (checkSync()) {
						lastSyncPos = in.getPos();
						break;
					} else {
						in.seek(in.getPos()-SYNC_SIZE+1);
					}
				}
			}
		}
		
		private void readHeader() throws IOException {
			SYNC_INTERVAL = in.readInt();
			in.read(this.sync, 0, SYNC_HASH_SIZE);
			lastSyncPos = in.getPos();
		}
		
		private boolean checkSync() throws IOException {
			int i;
			escape = in.readInt();
			if (escape == SYNC_ESCAPE) {
				in.read(checkSync, 0, SYNC_HASH_SIZE);
				for (i = 0; i < SYNC_HASH_SIZE; i++) {
					if (checkSync[i] != sync[i]) {
						break;
					}
				}
				if (i != SYNC_HASH_SIZE) {
					return false;
				}
			} else {
				in.seek(in.getPos()+SYNC_HASH_SIZE);
				return false;
			}
			return true;
		}

		@Override
		public Tuple next() throws IOException {
			if (in.available() == 0) {
				return null;
			}
			
			// check sync
			if (checkSync()) {
				if (in.getPos() >= end) {
					return null;
				}
				lastSyncPos = in.getPos();
			} else {
				in.seek(in.getPos()-SYNC_SIZE);
			}
			
			if (in.available() == 0) {
				return null;
			}
			
			int i;
			VTuple tuple = new VTuple(schema.getColumnNum());

			boolean [] contains = new boolean[schema.getColumnNum()];
			for (i = 0; i < schema.getColumnNum(); i++) {
				contains[i] = in.readBoolean();
			}

			Column col = null;
			for (i = 0; i < schema.getColumnNum(); i++) {
				if (contains[i]) {
					col = schema.getColumn(i);
					switch (col.getDataType()) {
					case BYTE:
						tuple.put(i, in.readByte());
						break;
					case SHORT:
						tuple.put(i, in.readShort());
						break;
					case INT:
						tuple.put(i, in.readInt());
						break;
					case LONG:
						tuple.put(i, in.readLong());
						break;
					case FLOAT:
						tuple.put(i, in.readFloat());
						break;
					case DOUBLE:
						tuple.put(i, in.readDouble());
						break;
					case STRING:
						short len = in.readShort();
						byte[] buf = new byte[len];
						in.read(buf, 0, len);
						tuple.put(i, new String(buf));
						break;
					case IPv4:
						byte[] ipv4 = new byte[4];
						in.read(ipv4, 0, 4);
						tuple.put(i, ipv4);
						break;
					default:
						break;
					}
				}
			}

			return tuple;
		}

		@Override
		public void reset() throws IOException {
			in.reset();
		}

		@Override
		public void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
		
	}
	
	public static class RawFileAppender implements Appender {
		
		private NtaConf conf;
		private FSDataOutputStream out;
		private long lastSyncPos;
		private Path path;
		private Schema schema;
		private FileSystem fs;
		private byte[] sync;
		
		public RawFileAppender(NtaConf conf, Store store) throws IOException {
			this.conf = conf;
			SYNC_INTERVAL = conf.getInt(NConstants.RAWFILE_SYNC_INTERVAL, SYNC_SIZE*100);
			schema = store.getSchema();
			path = new Path(store.getURI().toString(), "data");
			fs = path.getFileSystem(conf);
			if (!fs.exists(path)) {
				fs.mkdirs(path);
			}
			sync = new byte[SYNC_HASH_SIZE];
			lastSyncPos = 0;
			init();
		}
		
		private void init() throws IOException {
			MessageDigest md;
			try {
				md = MessageDigest.getInstance("MD5");
				md.update((path.toString()+System.currentTimeMillis()).getBytes());
				sync = md.digest();
			} catch (NoSuchAlgorithmException e) {
				LOG.error(e);
			}
			if (fs.exists(new Path(path, "table.raw"))) {
				throw new ReadOnlyException();
			}
			out = fs.create(new Path(path, "table.raw"));
			writeHeader();
		}
		
		private void writeHeader() throws IOException {
			out.writeInt(SYNC_INTERVAL);
			out.write(sync);
			out.flush();
			lastSyncPos = out.getPos();
		}
		
		@Override
		public void addTuple(Tuple t) throws IOException {
			checkAndWriteSync();
			Column col = null;
			for (int i = 0; i < schema.getColumnNum(); i++) {
				out.writeBoolean(t.contains(i));
			}
			for (int i = 0; i < schema.getColumnNum(); i++) {
				if (t.contains(i)) {
					col = schema.getColumn(i);
					switch (col.getDataType()) {
					case BYTE:
						out.writeByte(t.getByte(i));
						break;
					case STRING:
						byte[] buf = t.getString(i).getBytes();
						if (buf.length > 256) {
							buf = new byte[256];
							byte[] str = t.getString(i).getBytes();
							System.arraycopy(str, 0, buf, 0, 256);
						} 
						out.writeShort(buf.length);
						out.write(buf, 0, buf.length);
						break;
					case SHORT:
						out.writeShort(t.getShort(i));
						break;
					case INT:
						out.writeInt(t.getInt(i));
						break;
					case LONG:
						out.writeLong(t.getLong(i));
						break;
					case FLOAT:
						out.writeFloat(t.getFloat(i));
						break;
					case DOUBLE:
						out.writeDouble(t.getDouble(i));
						break;
					case IPv4:
						out.write(t.getIPv4Bytes(i));
						break;
					case IPv6:
						out.write(t.getIPv6Bytes(i));
						break;
					default:
						break;
					}
				}
			}
		}

		@Override
		public void flush() throws IOException {
			out.flush();
		}

		@Override
		public void close() throws IOException {
			if (out != null) {
				sync();
				out.flush();
				out.close();
			}
		}
		
		private void sync() throws IOException {
			if (lastSyncPos != out.getPos()) {
				out.writeInt(SYNC_ESCAPE);
				out.write(sync);
				lastSyncPos = out.getPos();
			}
		}
		
		synchronized void checkAndWriteSync() throws IOException {
			if (out.getPos() >= lastSyncPos + SYNC_INTERVAL) {
				sync();
			}
		}
		
	}
}
