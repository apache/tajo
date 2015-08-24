package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;

public class IndexExecutorUtil {

  public static String getIndexFileName(FragmentProto fragmentProto) {
    FileFragment fileFragment = FragmentConvertor.convert(FileFragment.class, fragmentProto);
    StringBuilder sb = new StringBuilder();
    sb.append(fileFragment.getPath().getName()).append(fileFragment.getStartKey()).append(fileFragment.getLength());
    return sb.toString();
  }
}
