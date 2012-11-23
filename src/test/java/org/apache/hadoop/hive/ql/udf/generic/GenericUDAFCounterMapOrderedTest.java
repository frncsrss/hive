package org.apache.hadoop.hive.ql.udf.generic;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.jointhegrid.hive_test.HiveTestServiceWithConstants;

public class GenericUDAFCounterMapOrderedTest extends HiveTestServiceWithConstants {
  
  public GenericUDAFCounterMapOrderedTest() throws IOException {
    super();
  }

  @Test
  public void testSimple() throws Exception {
    Path p = createInputFile(FILE,
        "3\n",
        "2\n",
        "1\n",
        "2\n",
        "2\n",
        "1\n",
        "2\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT)");
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFCounterMapOrdered.class.getName()));

    client.execute(FROM_TABLE + " SELECT f(col1)");
    assertEquals("{1:2,2:4,3:1}", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }

  @Test
  public void testGroupBy() throws Exception {
    Path p = createInputFile(FILE,
        "2138,3\n",
        "2138,2\n",
        "2138,1\n",
        "2138,2\n",
        "2138,1\n",
        "2138,1\n",
        "2140,2\n",
        "2140,3\n",
        "2140,1\n",
        "2140,2\n",
        "2140,2\n",
        "2140,5\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFCounterMapOrdered.class.getName()));

    client.execute(FROM_TABLE + " SELECT col1, f(col2) GROUP BY col1");
    assertEquals("2138\t{1:3,2:2,3:1}", client.fetchOne());
    assertEquals("2140\t{1:1,2:3,3:1,5:1}", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }
}
