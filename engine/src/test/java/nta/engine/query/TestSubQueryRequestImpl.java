package nta.engine.query;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.StoreType;
import nta.engine.LeafServerProtos.SubQueryRequestProto;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestSubQueryRequestImpl {

	@Test
	public void test() throws URISyntaxException {
	  Schema schema = new Schema();
	  TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
	  
		ArrayList<Fragment> tablets = new ArrayList<Fragment>();
		tablets.add(new Fragment("test1_1",new Path("test1"), meta, 0, 1));
		tablets.add(new Fragment("test1_2",new Path("test2"), meta, 1, 2));
		tablets.add(new Fragment("test1_3",new Path("test3"), meta, 2, 3));
		tablets.add(new Fragment("test1_4",new Path("test4"), meta, 3, 4));
		tablets.add(new Fragment("test1_5",new Path("test5"), meta, 4, 5));
		
		SubQueryRequest req1 = new SubQueryRequestImpl(tablets, new URI("out1"), "select test1", "table1");
		
		SubQueryRequestProto.Builder builder = SubQueryRequestProto.newBuilder();
		for (int i = 0; i < tablets.size(); i++) {
			builder.addTablets(tablets.get(i).getProto());
		}
		builder.setDest("out1");
		builder.setQuery("select test1");
		builder.setTableName("table1");
		SubQueryRequest req2 = new SubQueryRequestImpl(builder.build());
		
		List<Fragment> t1 = req1.getFragments();
		List<Fragment> t2 = req2.getFragments();
		assertEquals(t1.size(), t2.size());
		
		for (int i = 0; i < t1.size(); i++) {
			assertEquals(t1.get(i), t2.get(i));
		}
		assertEquals(req1.getOutputDest(), req2.getOutputDest());
		assertEquals(req1.getQuery(), req2.getQuery());
		assertEquals(req1.getTableName(), req2.getTableName());
	}
}
