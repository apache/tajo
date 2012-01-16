package nta.engine.query;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import nta.engine.LeafServerProtos.SubQueryRequestProto;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.Tablet;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestSubQueryRequestImpl {

	@Test
	public void test() throws URISyntaxException {
		ArrayList<Tablet> tablets = new ArrayList<Tablet>();
		tablets.add(new Tablet(new Path("test1"), "test1", 0, 1));
		tablets.add(new Tablet(new Path("test2"), "test2", 1, 2));
		tablets.add(new Tablet(new Path("test3"), "test3", 2, 3));
		tablets.add(new Tablet(new Path("test4"), "test4", 3, 4));
		tablets.add(new Tablet(new Path("test5"), "test5", 4, 5));
		
		SubQueryRequest req1 = new SubQueryRequestImpl(tablets, new URI("out1"), "select test1", "table1");
		
		SubQueryRequestProto.Builder builder = SubQueryRequestProto.newBuilder();
		for (int i = 0; i < tablets.size(); i++) {
			builder.addTablets(tablets.get(i).getProto());
		}
		builder.setDest("out1");
		builder.setQuery("select test1");
		builder.setTableName("table1");
		SubQueryRequest req2 = new SubQueryRequestImpl(builder.build());
		
		List<Tablet> t1 = req1.getTablets();
		List<Tablet> t2 = req2.getTablets();
		assertEquals(t1.size(), t2.size());
		
		for (int i = 0; i < t1.size(); i++) {
			assertEquals(t1.get(i), t2.get(i));
		}
		assertEquals(req1.getOutputDest(), req2.getOutputDest());
		assertEquals(req1.getQuery(), req2.getQuery());
		assertEquals(req1.getTableName(), req2.getTableName());
	}
}
