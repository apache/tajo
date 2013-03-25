/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.storage.rcfile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.Test;
import tajo.conf.TajoConf;
import tajo.datum.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * TestRCFile.
 *
 */
public class TestRCFile {

  private static final Log LOG = LogFactory.getLog(TestRCFile.class);

  private static Configuration conf = new Configuration();

  private static Path file;

  private static FileSystem fs;

  private static Properties tbl;

  static {
    try {
      fs = FileSystem.getLocal(conf);
      Path dir = new Path(System.getProperty("test.data.dir", ".") + "/mapred");
      file = new Path(dir, "test_rcfile");
      fs.delete(dir, true);
      // the SerDe part is from TestLazySimpleSerDe
      //serDe = new ColumnarSerDe();
      // Create the SerDe
      tbl = createProperties();
      //serDe.initialize(conf, tbl);
    } catch (Exception e) {
    }
  }

  public class Constants {
    public static final String SERIALIZATION_FORMAT = "serialization.format";

    public static final String SERIALIZATION_NULL_FORMAT = "serialization.null.format";
  }

  // Data

  private static Writable[] expectedFieldsData = {
      new ByteWritable((byte) 123), new ShortWritable((short) 456),
      new IntWritable(789), new LongWritable(1000), new DoubleWritable(5.3),
      new Text("hive and hadoop"), null, null};

  private static Object[] expectedPartitalFieldsData = {null, null,
      new IntWritable(789), new LongWritable(1000), null, null, null, null};
  private static BytesRefArrayWritable patialS = new BytesRefArrayWritable();

  private static byte[][] bytesArray = null;

  private static BytesRefArrayWritable s = null;
  static {
    try {
      bytesArray = new byte[][] {"123".getBytes("UTF-8"),
          "456".getBytes("UTF-8"), "789".getBytes("UTF-8"),
          "1000".getBytes("UTF-8"), "5.3".getBytes("UTF-8"),
          "hive and hadoop".getBytes("UTF-8"), new byte[0],
          "NULL".getBytes("UTF-8")};
      s = new BytesRefArrayWritable(bytesArray.length);
      s.set(0, new BytesRefWritable("123".getBytes("UTF-8")));
      s.set(1, new BytesRefWritable("456".getBytes("UTF-8")));
      s.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
      s.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
      s.set(4, new BytesRefWritable("5.3".getBytes("UTF-8")));
      s.set(5, new BytesRefWritable("hive and hadoop".getBytes("UTF-8")));
      s.set(6, new BytesRefWritable("NULL".getBytes("UTF-8")));
      s.set(7, new BytesRefWritable("NULL".getBytes("UTF-8")));

      // partial test init
      patialS.set(0, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(1, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
      patialS.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
      patialS.set(4, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(5, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(6, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(7, new BytesRefWritable("NULL".getBytes("UTF-8")));

    } catch (UnsupportedEncodingException e) {
    }
  }

  @Test
  public void testSimpleReadAndWrite() throws IOException {
    fs.delete(file, true);

    Datum [] r1 = new Datum[7];
    int idx = 0;
    r1[idx++] = DatumFactory.createInt(123);
    r1[idx++] = DatumFactory.createLong(456);
    r1[idx++] = DatumFactory.createFloat(7.89f);
    r1[idx++] = DatumFactory.createDouble(10.11d);
    r1[idx] = DatumFactory.createString("tajo and hadoop");

//    byte[][] record_1 = {
//        "123".getBytes("UTF-8"), "456".getBytes("UTF-8"),
//        "789".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
//        "5.3".getBytes("UTF-8"), "hive and hadoop".getBytes("UTF-8"),
//        new byte[0], "NULL".getBytes("UTF-8")};
    System.out.println("Original size: " + r1[4].asByteArray().length);
    byte [][] record_1 = {
        r1[0].asByteArray(),
        r1[1].asByteArray(),
        r1[2].asByteArray(),
        r1[3].asByteArray(),
        r1[4].asByteArray(),
        new byte[0],
        "NULL".getBytes("UTF-8")
    };

    Datum [] r2 = new Datum[7];
    idx = 0;
    r2[idx++] = DatumFactory.createInt(100);
    r2[idx++] = DatumFactory.createLong(200);
    r2[idx++] = DatumFactory.createFloat(5.3f);
    r2[idx++] = DatumFactory.createDouble(11.12d);
    r2[idx] = DatumFactory.createString("the second str");

    byte [][] record_2 = {
        r2[0].asByteArray(),
        r2[1].asByteArray(),
        r2[2].asByteArray(),
        r2[3].asByteArray(),
        r2[4].asByteArray(),
        new byte[0],
        "NULL".getBytes("UTF-8")
    };

//    byte[][] record_2 = {"100".getBytes("UTF-8"), "200".getBytes("UTF-8"),
//        "123".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
//        "5.3".getBytes("UTF-8"), "hive and hadoop".getBytes("UTF-8"),
//        new byte[0], "NULL".getBytes("UTF-8")};


    conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, 7);
    RCFile.Writer writer =
        new RCFile.Writer(fs, conf, file, null,
            RCFile.createMetadata(new Text("apple"),
                new Text("block"),
                new Text("cat"),
                new Text("dog")),
            new DefaultCodec());
    BytesRefArrayWritable bytes = new BytesRefArrayWritable(record_1.length);
    for (int i = 0; i < record_1.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record_1[i], 0,
          record_1[i].length);
      bytes.set(i, cu);
    }
    writer.append(bytes);
    bytes.clear();
    for (int i = 0; i < record_2.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record_2[i], 0,
          record_2[i].length);
      bytes.set(i, cu);
    }
    writer.append(bytes);
    writer.close();

//    Object[] expectedRecord_1 = {new ByteWritable((byte) 123),
//        new ShortWritable((short) 456), new IntWritable(789),
//        new LongWritable(1000), new DoubleWritable(5.3),
//        new Text("hive and hadoop"), null, null};
//
//    Object[] expectedRecord_2 = {new ByteWritable((byte) 100),
//        new ShortWritable((short) 200), new IntWritable(123),
//        new LongWritable(1000), new DoubleWritable(5.3),
//        new Text("hive and hadoop"), null, null};

    ColumnProjectionUtils.setFullyReadColumns(conf);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);
    assertEquals(new Text("block"),
        reader.getMetadata().get(new Text("apple")));
    assertEquals(new Text("block"),
        reader.getMetadataValueOf(new Text("apple")));
    assertEquals(new Text("dog"),
        reader.getMetadataValueOf(new Text("cat")));
    LongWritable rowID = new LongWritable();

    reader.next(rowID);
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    reader.getCurrentRow(cols);
    cols.resetValid(7);
    assertEquals(r1[0], new IntDatum(cols.get(0).getBytesCopy()));
    assertEquals(r1[1], new LongDatum(cols.get(1).getBytesCopy()));
    assertEquals(r1[2], new FloatDatum(cols.get(2).getBytesCopy()));
    assertEquals(r1[3], new DoubleDatum(cols.get(3).getBytesCopy()));
    assertEquals(r1[4], new StringDatum(cols.get(4).getBytesCopy()));


    reader.next(rowID);
    cols = new BytesRefArrayWritable();
    reader.getCurrentRow(cols);
    cols.resetValid(7);
    assertEquals(r2[0], new IntDatum(cols.get(0).getBytesCopy()));
    assertEquals(r2[1], new LongDatum(cols.get(1).getBytesCopy()));
    assertEquals(r2[2], new FloatDatum(cols.get(2).getBytesCopy()));
    assertEquals(r2[3], new DoubleDatum(cols.get(3).getBytesCopy()));
    assertEquals(r2[4], new StringDatum(cols.get(4).getBytesCopy()));
    /*
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());
      for (int j = 0; j < fieldRefs.size(); j++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(j));
        Object standardWritableData = ObjectInspectorUtils
            .copyToStandardObject(fieldData, fieldRefs.get(j)
                .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        if (i == 0) {
          assertEquals("Field " + i, standardWritableData, expectedRecord_1[j]);
        } else {
          assertEquals("Field " + i, standardWritableData, expectedRecord_2[j]);
        }
      }
    }*/

    reader.close();
  }

  /*
  public void testReadCorruptFile() throws IOException {
    fs.delete(file, true);

    byte[][] record = {null, null, null, null, null, null, null, null};

    RCFileOutputFormat.setColumnNumber(conf, expectedFieldsData.length);
    RCFile.Appender writer = new RCFile.Appender(fs, conf, file, null,
        new DefaultCodec());
    BytesRefArrayWritable bytes = new BytesRefArrayWritable(record.length);
    final int recCount = 100;
    Random rand = new Random();
    for (int recIdx = 0; recIdx < recCount; recIdx++) {
      for (int i = 0; i < record.length; i++) {
        record[i] = new Integer(rand.nextInt()).toString().getBytes("UTF-8");
      }
      for (int i = 0; i < record.length; i++) {
        BytesRefWritable cu = new BytesRefWritable(record[i], 0,
            record[i].length);
        bytes.set(i, cu);
      }
      writer.append(bytes);
      bytes.clear();
    }
    writer.close();

    // Insert junk in middle of file. Assumes file is on local disk.
    RandomAccessFile raf = new RandomAccessFile(file.toUri().getPath(), "rw");
    long corruptOffset = raf.length() / 2;
    LOG.info("corrupting " + raf + " at offset " + corruptOffset);
    raf.seek(corruptOffset);
    raf.writeBytes("junkjunkjunkjunkjunkjunkjunkjunk");
    raf.close();

    // Set the option for tolerating corruptions. The read should succeed.
    Configuration tmpConf = new Configuration(conf);
    tmpConf.setBoolean("hive.io.rcfile.tolerate.corruptions", true);
    RCFile.Reader reader = new RCFile.Reader(fs, file, tmpConf);

    LongWritable rowID = new LongWritable();

    while (true) {
      boolean more = reader.next(rowID);
      if (!more) {
        break;
      }
      BytesRefArrayWritable cols = new BytesRefArrayWritable();
      reader.getCurrentRow(cols);
      cols.resetValid(8);
    }

    reader.close();
  }
  */

  /*
  public void testWriteAndFullyRead() throws IOException {
    writeTest(fs, 10000, file, bytesArray);
    fullyReadTest(fs, 10000, file);
  }

  public void testWriteAndPartialRead() throws IOException {
    writeTest(fs, 10000, file, bytesArray);
    partialReadTest(fs, 10000, file);
  }*/

  /** For debugging and testing. */ /*
  public static void main(String[] args) throws Exception {
    int count = 10000;
    boolean create = true;

    String usage = "Usage: RCFile " + "[-count N]" + " file";
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    try {
      for (int i = 0; i < args.length; ++i) { // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else {
          // file is required parameter
          file = new Path(args[i]);
        }
      }

      if (file == null) {
        System.err.println(usage);
        System.exit(-1);
      }

      LOG.info("count = " + count);
      LOG.info("create = " + create);
      LOG.info("file = " + file);

      TestRCFile test = new TestRCFile();

      // test.performanceTest();

      test.testSimpleReadAndWrite();

      test.writeTest(fs, count, file, bytesArray);
      test.fullyReadTest(fs, count, file);
      test.partialReadTest(fs, count, file);
      System.out.println("Finished.");
    } finally {
      fs.close();
    }
  }*/

  private void writeTest(FileSystem fs, int count, Path file,
                         byte[][] fieldsData) throws IOException {
    writeTest(fs, count, file, fieldsData, conf);
  }

  private void writeTest(FileSystem fs, int count, Path file,
                         byte[][] fieldsData, Configuration conf) throws IOException {
    fs.delete(file, true);

    conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, fieldsData.length);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null,
        new DefaultCodec());

    BytesRefArrayWritable bytes = new BytesRefArrayWritable(fieldsData.length);
    for (int i = 0; i < fieldsData.length; i++) {
      BytesRefWritable cu;
      cu = new BytesRefWritable(fieldsData[i], 0, fieldsData[i].length);
      bytes.set(i, cu);
    }

    for (int i = 0; i < count; i++) {
      writer.append(bytes);
    }
    writer.close();
    long fileLen = fs.getFileStatus(file).getLen();
    System.out.println("The file size of RCFile with " + bytes.size()
        + " number columns and " + count + " number rows is " + fileLen);
  }

  private static Properties createProperties() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
        "abyte,ashort,aint,along,adouble,astring,anullint,anullstring");
    tbl.setProperty("columns.types",
        "tinyint:smallint:int:bigint:double:string:int:string");
    tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }

  /*
  public void fullyReadTest(FileSystem fs, int count, Path file)
      throws IOException {
    LOG.debug("reading " + count + " records");
    long start = System.currentTimeMillis();
    ColumnProjectionUtils.setFullyReadColumns(conf);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    int actualRead = 0;
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());
      for (int i = 0; i < fieldRefs.size(); i++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        Object standardWritableData = ObjectInspectorUtils
            .copyToStandardObject(fieldData, fieldRefs.get(i)
                .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        assertEquals("Field " + i, standardWritableData, expectedFieldsData[i]);
      }
      // Serialize
      assertEquals(
          "Class of the serialized object should be BytesRefArrayWritable",
          BytesRefArrayWritable.class, serDe.getSerializedClass());
      BytesRefArrayWritable serializedText = (BytesRefArrayWritable) serDe
          .serialize(row, oi);
      assertEquals("Serialized data", s, serializedText);
      actualRead++;
    }
    reader.close();
    assertEquals("Expect " + count + " rows, actual read " + actualRead,
        actualRead, count);
    long cost = System.currentTimeMillis() - start;
    LOG.debug("reading fully costs:" + cost + " milliseconds");
  }

  private void partialReadTest(FileSystem fs, int count, Path file)
      throws IOException {
    LOG.debug("reading " + count + " records");
    long start = System.currentTimeMillis();
    java.util.ArrayList<Integer> readCols = new java.util.ArrayList<Integer>();
    readCols.add(Integer.valueOf(2));
    readCols.add(Integer.valueOf(3));
    ColumnProjectionUtils.setReadColumnIDs(conf, readCols);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable cols = new BytesRefArrayWritable();

    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());

      for (int i : readCols) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        Object standardWritableData = ObjectInspectorUtils
            .copyToStandardObject(fieldData, fieldRefs.get(i)
                .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        assertEquals("Field " + i, standardWritableData,
            expectedPartitalFieldsData[i]);
      }

      assertEquals(
          "Class of the serialized object should be BytesRefArrayWritable",
          BytesRefArrayWritable.class, serDe.getSerializedClass());
      BytesRefArrayWritable serializedBytes = (BytesRefArrayWritable) serDe
          .serialize(row, oi);
      assertEquals("Serialized data", patialS, serializedBytes);
    }
    reader.close();
    long cost = System.currentTimeMillis() - start;
    LOG.debug("reading fully costs:" + cost + " milliseconds");
  }

  public void testSynAndSplit() throws IOException {
    splitBeforeSync();
    splitRightBeforeSync();
    splitInMiddleOfSync();
    splitRightAfterSync();
    splitAfterSync();
  }

  private void splitBeforeSync() throws IOException {
    writeThenReadByRecordReader(600, 1000, 2, 1, null);
  }

  private void splitRightBeforeSync() throws IOException {
    writeThenReadByRecordReader(500, 1000, 2, 17750, null);
  }

  private void splitInMiddleOfSync() throws IOException {
    writeThenReadByRecordReader(500, 1000, 2, 17760, null);

  }

  private void splitRightAfterSync() throws IOException {
    writeThenReadByRecordReader(500, 1000, 2, 17770, null);
  }

  private void splitAfterSync() throws IOException {
    writeThenReadByRecordReader(500, 1000, 2, 19950, null);
  } */

  /*
  private void writeThenReadByRecordReader(int intervalRecordCount,
                                           int writeCount, int splitNumber, long minSplitSize, CompressionCodec codec)
      throws IOException {
    Path testDir = new Path(System.getProperty("test.data.dir", ".")
        + "/mapred/testsmallfirstsplit");
    Path testFile = new Path(testDir, "test_rcfile");
    fs.delete(testFile, true);
    Configuration cloneConf = new Configuration(conf);
    cloneConf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, bytesArray.length);
    cloneConf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, intervalRecordCount);

    RCFile.Appender writer = new RCFile.Appender(fs, cloneConf, testFile, null, codec);

    BytesRefArrayWritable bytes = new BytesRefArrayWritable(bytesArray.length);
    for (int i = 0; i < bytesArray.length; i++) {
      BytesRefWritable cu = null;
      cu = new BytesRefWritable(bytesArray[i], 0, bytesArray[i].length);
      bytes.set(i, cu);
    }
    for (int i = 0; i < writeCount; i++) {
      if (i == intervalRecordCount) {
        System.out.println("write position:" + writer.getLength());
      }
      writer.append(bytes);
    }
    writer.close();

    RCFileInputFormat inputFormat = new RCFileInputFormat();
    JobConf jonconf = new JobConf(cloneConf);
    jonconf.set("mapred.input.dir", testDir.toString());
    jonconf.setLong("mapred.min.split.size", minSplitSize);
    InputSplit[] splits = inputFormat.getSplits(jonconf, splitNumber);
    assertEquals("splits length should be " + splitNumber, splits.length, splitNumber);
    int readCount = 0;
    for (int i = 0; i < splits.length; i++) {
      int previousReadCount = readCount;
      RecordReader rr = inputFormat.getRecordReader(splits[i], jonconf, Reporter.NULL);
      Object key = rr.createKey();
      Object value = rr.createValue();
      while (rr.next(key, value)) {
        readCount++;
      }
      System.out.println("The " + i + "th split read "
          + (readCount - previousReadCount));
    }
    assertEquals("readCount should be equal to writeCount", writeCount,
        readCount);
  } */


  // adopted Hadoop-5476 (calling new SequenceFile.Reader(...) leaves an
  // InputStream init, if the given sequence file is broken) to RCFile
  private static class TestFSDataInputStream extends FSDataInputStream {
    private boolean closed = false;

    private TestFSDataInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }

    public boolean isClosed() {
      return closed;
    }
  }

  @Test
  public void testCloseForErroneousRCFile() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    // create an empty file (which is not a valid rcfile)
    Path path = new Path(System.getProperty("test.build.data", "target")
        + "/broken.rcfile");
    fs.create(path).close();
    // try to create RCFile.Reader
    final TestFSDataInputStream[] openedFile = new TestFSDataInputStream[1];
    try {
      new RCFile.Reader(fs, path, conf) {
        // this method is called by the RCFile.Reader constructor, overwritten,
        // so we can access the opened file
        @Override
        protected FSDataInputStream openFile(FileSystem fs, Path file,
                                             int bufferSize, long length) throws IOException {
          final InputStream in = super.openFile(fs, file, bufferSize, length);
          openedFile[0] = new TestFSDataInputStream(in);
          return openedFile[0];
        }
      };
      fail("IOException expected.");
    } catch (IOException expected) {
    }
    assertNotNull(path + " should have been opened.", openedFile[0]);
    assertTrue("InputStream for " + path + " should have been closed.",
        openedFile[0].isClosed());
  }



  public void testRCFileHeader(char[] expected, Configuration conf)
      throws IOException {

    writeTest(fs, 10000, file, bytesArray, conf);
    DataInputStream di = fs.open(file, 10000);
    byte[] bytes = new byte[3];
    di.read(bytes);
    for (int i = 0; i < expected.length; i++) {
      assertTrue("Headers did not match", bytes[i] == expected[i]);
    }
    di.close();
  }

  @Test
  public void testNonExplicitRCFileHeader() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(TajoConf.ConfVars.HIVEUSEEXPLICITRCFILEHEADER.varname, false);
    char[] expected = new char[] {'S', 'E', 'Q'};
    testRCFileHeader(expected, conf);
  }

  @Test
  public void testExplicitRCFileHeader() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(TajoConf.ConfVars.HIVEUSEEXPLICITRCFILEHEADER.varname, true);
    char[] expected = new char[] {'R', 'C', 'F'};
    testRCFileHeader(expected, conf);
  }
}
