package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.util.FileUtil;

import java.io.IOException;

public class StorageUtil {
  public static int getRowByteSize(Schema schema) {
    int sum = 0;
    for(Column col : schema.getColumns()) {
      sum += StorageUtil.getColByteSize(col);
    }

    return sum;
  }

  public static int getColByteSize(Column col) {
    switch(col.getDataType()) {
    case BOOLEAN: return 1;
    case CHAR: return 1;
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

  public static void writeTableMeta(Configuration conf, Path tableroot, 
      TableMeta meta) throws IOException {
    FileSystem fs = tableroot.getFileSystem(conf);
    FSDataOutputStream out = fs.create(new Path(tableroot, ".meta"));
    FileUtil.writeProto(out, meta.getProto());
    out.flush();
    out.close();
  }
  
  public static Path concatPath(String parent, String...childs) {
    return concatPath(new Path(parent), childs);
  }
  
  public static Path concatPath(Path parent, String...childs) {
    StringBuilder sb = new StringBuilder();
    
    for(int i=0; i < childs.length; i++) {      
      sb.append(childs[i]);
      if(i < childs.length - 1)
        sb.append("/");
    }
    
    return new Path(parent, sb.toString());
  }
}
