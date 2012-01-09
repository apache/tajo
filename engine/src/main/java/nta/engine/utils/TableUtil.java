package nta.engine.utils;

import java.io.FileNotFoundException;
import java.io.IOException;

import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.TableProto;
import nta.util.FileUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TableUtil {
  public static TableMeta getTableMeta(Configuration conf, Path tablePath) throws IOException {
    TableMeta meta = null;
    
    FileSystem fs = tablePath.getFileSystem(conf);
    
    Path tableMetaPath = new Path(tablePath, ".meta");
    if(!fs.exists(tableMetaPath)) {
      throw new FileNotFoundException(".meta file not found in "+tablePath.toString());
    }
    
    FSDataInputStream tableMetaIn = fs.open(tableMetaPath);

    TableProto tableProto = (TableProto) FileUtil.loadProto(tableMetaIn, 
      TableProto.getDefaultInstance());
    meta = new TableMetaImpl(tableProto);

    return meta;
  }
}
