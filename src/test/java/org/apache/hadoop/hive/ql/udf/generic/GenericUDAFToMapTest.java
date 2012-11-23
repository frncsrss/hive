package org.apache.hadoop.hive.ql.udf.generic;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.jointhegrid.hive_test.HiveTestServiceWithConstants;

public class GenericUDAFToMapTest extends HiveTestServiceWithConstants {

  public GenericUDAFToMapTest() throws IOException {
    super();
  }


  @Test
  public void testSimple() throws Exception {
    Path p = createInputFile(FILE,
        "1,11\n",
        "2,12\n",
        "3,13\n",
        "4,14\n",
        "1,10\n",
        "2,20\n",
        "5,40\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToMap.class.getName()));

    client.execute(FROM_TABLE + " SELECT f(col1, col2)");
    assertEquals("{1:10,2:20,3:13,4:14,5:40}", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }

  @Test
  public void testGroupBy() throws Exception {
    Path p = createInputFile(FILE,
        "2138,1,11\n",
        "2138,2,12\n",
        "2138,3,13\n",
        "2138,4,14\n",
        "2140,1,10\n",
        "2140,2,20\n",
        "2140,3,30\n",
        "2140,5,40\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT, col3 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToMap.class.getName()));

    client.execute(FROM_TABLE + " SELECT col1, f(col2, col3) GROUP BY col1");
    assertEquals("2138\t{1:11,2:12,3:13,4:14}", client.fetchOne());
    assertEquals("2140\t{1:10,2:20,3:30,5:40}", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }

  @Test
  public void testNested() throws Exception {
    Path p = createInputFile(FILE,
        "2138,1,11\n",
        "2138,1,12\n",
        "2138,2,13\n",
        "2138,3,14\n",
        "2140,1,10\n",
        "2140,5,20\n",
        "2140,3,30\n",
        "2140,5,40\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT, col3 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToMap.class.getName()));

    client.execute(
        "FROM("
            + FROM_TABLE + " SELECT col1, col2, sum(col3) AS sum GROUP BY col1, col2"
        + " )a"
        + " SELECT col1, f(col2, sum)"
        + " GROUP BY col1");
    assertEquals("2138\t{1:23,2:13,3:14}", client.fetchOne());
    assertEquals("2140\t{1:10,3:30,5:60}", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }
}
