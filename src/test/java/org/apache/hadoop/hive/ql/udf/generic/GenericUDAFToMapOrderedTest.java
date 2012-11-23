package org.apache.hadoop.hive.ql.udf.generic;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.jointhegrid.hive_test.HiveTestServiceWithConstants;

public class GenericUDAFToMapOrderedTest extends HiveTestServiceWithConstants {

  public GenericUDAFToMapOrderedTest() throws IOException {
    super();
  }

  @Test
  public void testSimple() throws Exception {
    Path p = createInputFile(FILE,
        "2138,3,11\n",
        "2138,2,12\n",
        "2138,1,13\n",
        "2138,4,14\n",
        "2140,5,10\n",
        "2140,2,20\n",
        "2140,4,30\n",
        "2140,1,40\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT, col3 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToMapOrdered.class.getName()));

    client.execute(FROM_TABLE+ " SELECT col1, f(col2, col3) GROUP BY col1");
    assertEquals("2138\t{1:13,2:12,3:11,4:14}", client.fetchOne());
    assertEquals("2140\t{1:40,2:20,4:30,5:10}", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }

}