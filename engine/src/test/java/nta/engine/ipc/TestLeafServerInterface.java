/**
 * 
 */
package nta.engine.ipc;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.LeafServerProtos.AssignTabletRequestProto;
import nta.engine.LeafServerProtos.ReleaseTabletRequestProto;
import nta.engine.LeafServerProtos.SubQueryRequestProto;
import nta.engine.LeafServerProtos.SubQueryResponseProto;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.query.SubQueryRequestImpl;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author jihoon
 *
 */
public class TestLeafServerInterface {
	ArrayList<SubQueryRequest> reqList = new ArrayList<SubQueryRequest>();
	
	@Before
	public void setup() throws URISyntaxException {
	  Schema schema = new Schema();
    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
		ArrayList<Fragment> tablets = new ArrayList<Fragment>();
		tablets.add(new Fragment("test1_1",new Path("test1"), meta, 0, 1));
    tablets.add(new Fragment("test1_2",new Path("test2"), meta, 1, 2));
    tablets.add(new Fragment("test1_3",new Path("test3"), meta, 2, 3));
    tablets.add(new Fragment("test1_4",new Path("test4"), meta, 3, 4));
    tablets.add(new Fragment("test1_5",new Path("test5"), meta, 4, 5));
		for (int i = 0; i < 10; i++) {
			reqList.add(new SubQueryRequestImpl(i, tablets, new URI("out"+i), "query"+i, "table"+i));
		}
	}
	
	@After
	public void terminate() {
		
	}

	@Test
	public void testSubQueryRequest() throws Exception {
		TestClient client = new TestClient();
		ProtoParamRpcServer server = NettyRpc.getProtoParamRpcServer(client, 
				new InetSocketAddress("localhost", 9001));
		server.start();
		
		LeafServerInterface leaf;
		for (SubQueryRequest req : reqList) {
			leaf = (LeafServerInterface) NettyRpc.getProtoParamBlockingRpcProxy(LeafServerInterface.class, 
					new InetSocketAddress("localhost", 9001));
			System.out.print("send request -> ");
			leaf.requestSubQuery(req.getProto());
		}
		
		for (int i = 0; i < reqList.size(); i++) {
			List<Fragment> t1 = reqList.get(i).getFragments();
			List<Fragment> t2 = client.reqList.get(i).getFragments();
			assertEquals(t1.size(), t2.size());
			for (int j = 0; j < t1.size(); j++) {
				assertEquals(t1.get(j), t2.get(j));
			}
			assertEquals(reqList.get(i).getOutputDest(), client.reqList.get(i).getOutputDest());
			assertEquals(reqList.get(i).getQuery(), client.reqList.get(i).getQuery());
			assertEquals(reqList.get(i).getTableName(), client.reqList.get(i).getTableName());
		}
	}
	
	public class TestClient implements LeafServerInterface {
		ArrayList<SubQueryRequest> reqList;
		
		public TestClient() {
			reqList = new ArrayList<SubQueryRequest>();
		}
		
		@Override
		public void shutdown(String why) {
			
		}

		@Override
		public void abort(String why, Throwable e) {
			
		}

		@Override
		public SubQueryResponseProto requestSubQuery(
				SubQueryRequestProto request) throws Exception {
			System.out.println("Receive Success!!!!!!!");
			SubQueryRequestImpl req = new SubQueryRequestImpl(request);
			reqList.add(req);
			return null;
		}

		@Override
		public void assignTablets(AssignTabletRequestProto request) {
			
		}

		@Override
		public void releaseTablets(ReleaseTabletRequestProto request) {
			
		}
		
	}
}
