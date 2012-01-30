package nta.cube;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;

/* test용 schema */
public class Schema_cube {

  public static void SetOriginSchema() {
    Schema schema = new Schema();
    // schema.addColumn("sys_uptime", DataType.LONG);//0
    // schema.addColumn("unix_secs", DataType.LONG);
    // schema.addColumn("unix_nsecs", DataType.LONG);
    // schema.addColumn("eng_type", DataType.BYTE);
    // schema.addColumn("end_id", DataType.BYTE);
    // schema.addColumn("itval", DataType.INT);
    //
    // schema.addColumn("srcaddColumnr", DataType.IPv4);
    // schema.addColumn("dstaddColumnr", DataType.IPv4);
    // schema.addColumn("src_net", DataType.INT);
    // schema.addColumn("dst_net", DataType.INT);
    // schema.addColumn("input", DataType.INT);//10
    // schema.addColumn("output", DataType.INT);
    // schema.addColumn("dPkts", DataType.LONG);
    // schema.addColumn("dOctets", DataType.LONG);
    // schema.addColumn("first", DataType.LONG);
    // schema.addColumn("last", DataType.LONG);//15
    // schema.addColumn("srcPort", DataType.INT);
    // schema.addColumn("dstPort", DataType.INT);
    // schema.addColumn("tcp_flags", DataType.BYTE);
    // schema.addColumn("prot", DataType.BYTE);
    // schema.addColumn("tos", DataType.BYTE);//20
    // schema.addColumn("tag", DataType.LONG);
    schema.addColumn("src_net", DataType.INT);
    schema.addColumn("dst_net", DataType.INT);
    schema.addColumn("dst_net2", DataType.INT);
    schema.addColumn("dst_net3", DataType.INT);

    Cons.ORIGIN_SCHEMA = schema;
  }
}
