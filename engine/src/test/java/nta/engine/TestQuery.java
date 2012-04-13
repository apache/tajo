package nta.engine;

import java.io.IOException;
import java.util.Random;

import nta.catalog.CatalogService;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.storage.CSVFile2;
import nta.util.FileUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;

/**
 * 
 * @author jihoon
 *
 */
public class TestQuery {

	Configuration conf;
	NtaTestingUtility util;
	NtaEngineMaster clusterMaster;
	MiniTajoCluster cluster;
	Schema schema;
	CatalogService catalog;
	
	final String TEST_PATH = "";

	@Before
	public void setup() throws Exception {
		// run cluster
		util = new NtaTestingUtility();
		util.startMiniCluster(10);
		conf = util.getConfiguration();
		cluster = util.getMiniNtaEngineCluster();
		FileSystem fs = util.getMiniDFSCluster().getFileSystem();

		int i, j;
		FSDataOutputStream fos;
		Path tbPath;

		schema = new Schema();
		schema.addColumn("id",DataType.INT);
		schema.addColumn("age",DataType.INT);
		schema.addColumn("name",DataType.STRING);

		TableMeta meta;

		String [] tuples = {
				"1,32,hyunsik",
				"2,29,jihoon",
				"3,28,jimin",
				"4,24,haemi"
		};

		clusterMaster = cluster.getMaster();
		clusterMaster.start();
		conf = new NtaConf(util.getConfiguration());
		catalog = clusterMaster.getCatalog();

		int tbNum = 2;
		int tupleNum;
		Random rand = new Random();

		for (i = 0; i < tbNum; i++) {
			tbPath = new Path(TEST_PATH+"/table"+i);
			if (fs.exists(tbPath)){
				fs.delete(tbPath, true);
			}
			fs.mkdirs(tbPath);
			fos = fs.create(new Path(tbPath, ".meta"));
			meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
			meta.putOption(CSVFile2.DELIMITER, ",");			
			FileUtil.writeProto(fos, meta.getProto());
			fos.close();

			fos = fs.create(new Path(tbPath, "data/table.csv"));
			tupleNum = 100;
			for (j = 0; j < tupleNum; j++) {
				fos.writeBytes(tuples[rand.nextInt(4)]+"\n");
			}
			fos.close();

			TableDesc desc = new TableDescImpl("table"+i, meta, tbPath);
			catalog.addTable(desc);
		}
	}

	@After
	public void terminate() throws IOException {
		util.shutdownMiniCluster();
	}
}
