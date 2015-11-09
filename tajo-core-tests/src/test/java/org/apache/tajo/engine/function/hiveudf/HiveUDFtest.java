package org.apache.tajo.engine.function.hiveudf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Description(name="multiplestr")
public class HiveUDFtest extends UDF {
  Text evaluate(Text str, IntWritable num) {
    String origin = str.toString();

    for (int i=0; i<num.get()-1; i++) {
      origin += origin;
    }

    return new Text(origin);
  }
}
