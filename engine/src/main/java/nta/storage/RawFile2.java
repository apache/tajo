package nta.storage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import nta.catalog.Column;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.storage.exception.AlreadyExistsStorageException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RawFile2 extends Storage {

  public static final Log LOG = LogFactory.getLog(RawFile2.class);

  public RawFile2(Configuration conf) {
    super(conf);
  }

  @Override
  public Appender getAppender(Schema schema, Path path)
  throws IOException {
    return new RawFileAppender(conf, schema, path);
  }

  @Override
  public Scanner openScanner(Schema schema, Fragment[] tablets)
  throws IOException {
    return new RawFileScanner(conf, schema, tablets);
  }

  private static final int SYNC_ESCAPE = -1;
  private static final int SYNC_HASH_SIZE = 16;
  private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE;
  public static int SYNC_INTERVAL;

  public static class RawFileScanner extends FileScanner {
	  
    private FSDataInputStream in;
    private SortedSet<Fragment> tabletSet;
    private Iterator<Fragment> tableIter;
    private Fragment curTablet;
    private FileSystem fs;
    private byte[] sync;
    private byte[] checkSync;
    private long start, end, headerPos, lastSyncPos; 
    private Options option;
    private long currentTupleOffset;

    public RawFileScanner(Configuration conf, final Schema schema, 
        final Fragment[] tablets) throws IOException {
      super(conf, schema, tablets);
      init();
    }

    public RawFileScanner(Configuration conf, final Schema schema, 
        final Fragment[] tablets, Options option) throws IOException {
      super(conf, schema, tablets);
      this.option = option;
      init();
    }

    private void init() throws IOException {
      this.tabletSet = new TreeSet<Fragment>();
      this.sync = new byte[SYNC_HASH_SIZE];
      this.checkSync = new byte[SYNC_HASH_SIZE];

      for (Fragment t: tablets) {
        this.tabletSet.add(t);
      }
      this.tableIter = tabletSet.iterator();
      openNextTablet();
    }

    private boolean openNextTablet() throws IOException {
      if (this.in != null) {
        this.in.close();
      }
      if (tableIter.hasNext()) {
        curTablet = tableIter.next();
        this.fs = curTablet.getPath().getFileSystem(this.conf);
        this.in = fs.open(curTablet.getPath());
        this.start = curTablet.getStartOffset();
        this.end = curTablet.getStartOffset() + curTablet.getLength();

        if (start == 0) { // read sync maker.
          readHeader();
        }
        if (!pageBuffer()) {
          return false;
        }
        return true;
      } else {
        return false;
      }
    }

    private void readHeader() throws IOException {
      SYNC_INTERVAL = in.readInt();
      in.read(this.sync, 0, SYNC_HASH_SIZE);
      lastSyncPos = in.getPos();
    }

    private boolean checkSync() throws IOException {
      in.readInt(); // escape
      in.read(checkSync, 0, SYNC_HASH_SIZE);  // sync
      if (!Arrays.equals(checkSync, sync)) {
        in.seek(in.getPos()-SYNC_SIZE);
        return false;
      } else {
        return true;
      }
    }

    private long pageStart, pageLen;
    private int tupleSize, pieceSize1, pieceSize2, bufferSize, defaultSize = 65536;
    private byte[] buffer;
    ByteArrayInputStream in_byte;
    DataInputStream in_buffer;     
    private boolean pageBuffer() throws IOException {      
      if (in.getPos() - SYNC_SIZE == start) { // first.
        tupleSize = checkTupleSize();
        if(defaultSize > tabletable()) {
        	buffer = new byte[(int)(tabletable())];
        } else {
        	buffer = new byte[defaultSize];
        }
        pageStart = in.getPos();
        in.read(buffer);
      } else if (in_buffer.available() > 0 || tabletable() > 0) { // tuple 이 잘림.
        pieceSize1 = in_buffer.available();      
        pieceSize2 = (int)tabletable();

        if (pieceSize1 + pieceSize2 < defaultSize) {
          bufferSize = pieceSize1 + pieceSize2;
          buffer = new byte[bufferSize];
          in_buffer.read(buffer, 0, pieceSize1);
          pageStart = in.getPos() - pieceSize1;
          in.read(buffer, pieceSize1, pieceSize2);
        } else {
          bufferSize = defaultSize;
          buffer = new byte[bufferSize];
          in_buffer.read(buffer, 0, pieceSize1);
          pageStart = in.getPos() - pieceSize1;
          in.read(buffer, pieceSize1, (bufferSize - pieceSize1));
        }
      } else {
        if (!openNextTablet()) {  // last.
          return false;
        }
      }
      pageLen = buffer.length;
      in_byte = new ByteArrayInputStream(buffer);
      in_buffer = new DataInputStream(in_byte);
      this.currentTupleOffset = 0;
//      checkSyncinPage();
      return true;
    }

    private boolean checkSyncinPage() throws IOException {  // page buffer 안의 sync maker 를 확인.
      in_buffer.mark(1);
      in_buffer.readInt();  // escape
      in_buffer.read(checkSync, 0, SYNC_HASH_SIZE);  // sync
      if (!Arrays.equals(checkSync, sync)) {
        in_buffer.reset();
        return false;
      } else {
        return true;
      }
    }

    private int checkTupleSize() throws IOException { // raw type 은 tuple 사이를 구분 할 수 없으므로, tuple size 를 예측해서 tuple 구분을 함.
      int i;
      long before = in.getPos();

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
            in.readByte();
            break;
          case SHORT:
            in.readShort();
            break;
          case INT:
            in.readInt();
            break;
          case LONG:
            in.readLong();
            break;
          case FLOAT:
            in.readFloat();
            break;
          case DOUBLE:
            in.readDouble();
            break;
          case STRING:
            short len = in.readShort();
            byte[] buf = new byte[len];
            in.read(buf, 0, len);
            break;
          case IPv4:
            byte[] ipv4 = new byte[4];
            in.read(ipv4, 0, 4);
            break;
          default:
            break;
          }
        }
      }
      long after = in.getPos();
      in.seek(before);
      return (int) (after - before);
    }

    @Override
    public void seek(long offset) throws IOException {
    	if ( offset >= this.pageStart + this.pageLen  ||
    			offset < this.pageStart) {
    		in.seek(offset);
    		in_byte.close();
    		in_buffer.close();
    		in_byte = new ByteArrayInputStream(new byte[0]);
    	    in_buffer = new DataInputStream(in_byte);
    		pageBuffer();
    	} else {
    		long bufferOffset = offset - this.pageStart;
    		if(this.currentTupleOffset == bufferOffset) {
    			
    		} else if( this.currentTupleOffset < bufferOffset) {
    			in_buffer.skip(bufferOffset - this.currentTupleOffset);
    			this.currentTupleOffset = bufferOffset;
    		} else {
    			in_buffer.close();
    			in_byte.close();
    			in_byte = new ByteArrayInputStream(buffer);
    			in_buffer = new DataInputStream(in_byte);
    			this.currentTupleOffset = bufferOffset;
    		}
    	}
    }
    
    @Override
    public long getNextOffset() {
    	return this.pageStart + this.currentTupleOffset;
    }
    
    @Override
    public long tabletable() throws IOException{
      return this.end - in.getPos();
    }
    
    @Override
    public Tuple next() throws IOException {
      if (in_buffer.available() == 0) { // page buffer 다 읽음.
        if (!pageBuffer()) {
          return null;
        }       
      }
      if (in_buffer.available() < tupleSize) {  // tuple 잘릴 것임. 
        if (!pageBuffer()) {
          return null;
        }
      }
      if (checkSyncinPage()) {  // page buffer 에 sync maker 가 있는지 확인.
    	this.currentTupleOffset += SYNC_SIZE;  
        if (in_buffer.available() == 0) { // sync maker 읽었더니 page buffer 다 읽음.
          if (!pageBuffer()) {
            return null;
          }
        }
        if (in_buffer.available() < tupleSize) { // sync maker 읽었더니 tuple 잘릴 것임.
          if (!pageBuffer()) {
            return null;
          }
        }
      }

      int i;
      VTuple tuple = new VTuple(schema.getColumnNum());

      boolean [] contains = new boolean[schema.getColumnNum()];
      for (i = 0; i < schema.getColumnNum(); i++) {
        contains[i] = in_buffer.readBoolean();
        this.currentTupleOffset += DatumFactory.createBool(true).size();
      }
      Column col = null;
      for (i = 0; i < schema.getColumnNum(); i++) {
    	Datum datum;
        if (contains[i]) {
          col = schema.getColumn(i);
          switch (col.getDataType()) {
          case BYTE:
        	datum = DatumFactory.createByte(in_buffer.readByte());
        	this.currentTupleOffset += datum.size();
            tuple.put(i, datum );
            break;
          case SHORT:
        	datum = DatumFactory.createShort(in_buffer.readShort());
        	this.currentTupleOffset += datum.size();
            tuple.put(i, datum );
            break;
          case INT:
            datum = DatumFactory.createInt(in_buffer.readInt());
        	this.currentTupleOffset += datum.size();
            tuple.put(i, datum );
            break;
          case LONG:
        	datum = DatumFactory.createLong(in_buffer.readLong());   
        	this.currentTupleOffset += datum.size();  
            tuple.put(i, datum );
            break;
          case FLOAT:
        	datum = DatumFactory.createFloat(in_buffer.readFloat());  
        	this.currentTupleOffset += datum.size();  
            tuple.put(i, datum);
            break;
          case DOUBLE:
        	datum = DatumFactory.createDouble(in_buffer.readDouble());
        	this.currentTupleOffset += datum.size();
            tuple.put(i, datum);
            break;
          case STRING:
        	this.currentTupleOffset += DatumFactory.createShort((short)0).size();  
            short len = in_buffer.readShort();
            byte[] buf = new byte[len];
            in_buffer.read(buf, 0, len);
            datum = DatumFactory.createString(new String(buf));
            this.currentTupleOffset += datum.size();
            tuple.put(i, datum);
            break;
          case IPv4:
        	byte[] ipv4 = new byte[4];
            in_buffer.read(ipv4, 0, 4);
            datum = DatumFactory.createIPv4(ipv4);
            this.currentTupleOffset += datum.size();
            tuple.put(i, datum);
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
    public Schema getSchema() {     
      return this.schema;
    }

    @Override
    public void close() throws IOException {
      if (in != null) {
        in.close();
      }
    }   
  }

  public static class RawFileAppender extends FileAppender {
    private FSDataOutputStream out;
    private long lastSyncPos;
    private FileSystem fs;
    private byte[] sync;

    public RawFileAppender(Configuration conf, final Schema schema, 
        final Path path) throws IOException {
      super(conf, schema, path);      

      fs = path.getFileSystem(conf);

      if (!fs.exists(path.getParent())) {
        throw new FileNotFoundException(path.toString());
      }

      if (fs.exists(path)) {
        throw new AlreadyExistsStorageException(path);
      }

      SYNC_INTERVAL = conf.getInt(NConstants.RAWFILE_SYNC_INTERVAL, SYNC_SIZE*100);     
      sync = new byte[SYNC_HASH_SIZE];
      lastSyncPos = 0;

      out = fs.create(path);

      MessageDigest md;
      try {
        md = MessageDigest.getInstance("MD5");
        md.update((path.toString()+System.currentTimeMillis()).getBytes());
        sync = md.digest();
      } catch (NoSuchAlgorithmException e) {
        LOG.error(e);
      }

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
						out.writeByte(t.getByte(i).asByte());
						break;
					case STRING:
						byte[] buf = t.getString(i).asByteArray();
						if (buf.length > 256) {
							buf = new byte[256];
							byte[] str = t.getString(i).asByteArray();
							System.arraycopy(str, 0, buf, 0, 256);
						} 
						out.writeShort(buf.length);
						out.write(buf, 0, buf.length);
						break;
					case SHORT:
						out.writeShort(t.getShort(i).asShort());
						break;
					case INT:
						out.writeInt(t.getInt(i).asInt());
						break;
					case LONG:
						out.writeLong(t.getLong(i).asLong());
						break;
					case FLOAT:
						out.writeFloat(t.getFloat(i).asFloat());
						break;
					case DOUBLE:
						out.writeDouble(t.getDouble(i).asDouble());
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

