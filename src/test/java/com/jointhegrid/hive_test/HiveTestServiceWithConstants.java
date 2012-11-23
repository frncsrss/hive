package com.jointhegrid.hive_test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public abstract class HiveTestServiceWithConstants extends HiveTestService {

  protected static final String FILE = "file_tmp";
  protected static final String TABLE = "table_tmp";

  protected static final String ADD_JAR = "ADD JAR ";
  protected static final String ADD_THIS_JAR = ADD_JAR + "target/hive-1.0-SNAPSHOT.jar";
  protected static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + TABLE;
  protected static final String CREATE_FUNCTION = "CREATE TEMPORARY FUNCTION ";
  protected static final String CREATE_FUNCTION_f = "CREATE TEMPORARY FUNCTION f AS '%s'";
  protected static final String DROP_TABLE = "DROP TABLE IF EXISTS " + TABLE;
  protected static final String FROM_TABLE = "FROM " + TABLE;
  protected static final String INTO_TABLE = " INTO TABLE " + TABLE;
  protected static final String LOAD_LOCAL_INTO_TABLE = "LOAD DATA LOCAL INPATH '%s'" + INTO_TABLE;
  protected static final String ROW_FORMAT_COMMA = " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

  public HiveTestServiceWithConstants() throws IOException {
    super();
  }

  protected Path createInputFile(String filename, String... lines) throws IOException {
    // Use the Hadoop filesystem API to create a data file.
    Path p = new Path(HiveTestService.ROOT_DIR, filename);
    FSDataOutputStream o = getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    for(String line: lines) {
      bw.write(line);
    }
    bw.close();
    return p;
  }

}
