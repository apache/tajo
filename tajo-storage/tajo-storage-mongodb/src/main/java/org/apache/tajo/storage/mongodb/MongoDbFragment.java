package org.apache.tajo.storage.mongodb;

import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.storage.fragment.Fragment;

/**
 * Created by janaka on 5/21/16.
 */
public class MongoDbFragment implements Fragment {

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public CatalogProtos.FragmentProto getProto() {
        return null;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public String[] getHosts() {
        return new String[0];
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
