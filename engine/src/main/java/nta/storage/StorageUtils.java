package nta.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.ipc.protocolrecords.Tablet;

public class StorageUtils {
	public static int getRowByteSize(Schema schema) {
		int sum = 0;
		for(Column col : schema.getColumns()) {
			sum += StorageUtils.getColByteSize(col);
		}
		
		return sum;
	}
	
	public static int getColByteSize(Column col) {
		switch(col.getDataType()) {
		case BOOLEAN: return 1;
		case BYTE: return 1;
		case SHORT: return 2;
		case INT: return 4;
		case LONG: return 8;
		case FLOAT: return 4;
		case DOUBLE: return 8;
		case IPv4: return 4;
		case IPv6: return 32;
		case STRING: return 256;
		default: return 0;
		}
	}
	
	public static Tablet[] reconstructTablets(Configuration conf, Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    long defaultBlockSize = fs.getDefaultBlockSize();

    List<Tablet> listTablets = new ArrayList<Tablet>();
    Tablet tablet = null;

    FileStatus[] fileLists = fs.listStatus(new Path(path, "data"));
    for (FileStatus file : fileLists) {
      long fileBlockSize = file.getLen();
      long start = 0;
      if (fileBlockSize > defaultBlockSize) {
        while (fileBlockSize > start) {
          tablet = new Tablet(path, file.getPath().getName(), start,
              defaultBlockSize);
          listTablets.add(tablet);
          start += defaultBlockSize;
        }
      } else {
        listTablets.add(new Tablet(path, file.getPath().getName(), 0,
            fileBlockSize));
      }
    }

    Tablet[] tablets = new Tablet[listTablets.size()];
    listTablets.toArray(tablets);
    
    return tablets;
  }
}
