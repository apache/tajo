package nta.cube;

import java.io.IOException;

import nta.storage.StorageManager;

import org.apache.hadoop.conf.Configuration;

/* 중간에 생기는 immediate file delete */
public class Delete {
  public static void delete() throws IOException {
    StorageManager sm = StorageManager.get(new Configuration(), Cons.datapath);
    sm.delete(Cons.immediatepath);
  }
}
