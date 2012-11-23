package org.apache.hadoop.hive.ql.udf.generic;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.jointhegrid.hive_test.HiveTestServiceWithConstants;

public class GenericUDAFToListTest extends HiveTestServiceWithConstants {

  public GenericUDAFToListTest() throws IOException {
    super();
  }

  @Test
  public void testSimple() throws Exception {
    Path p = createInputFile(FILE,
        "1\n",
        "2\n",
        "3\n",
        "2\n",
        "2\n",
        "1\n",
        "2\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT)");
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToList.class.getName()));

    client.execute(FROM_TABLE + " SELECT f(col1)");
    assertEquals("[1,2,3,2,2,1,2]", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }

  @Test
  public void testGroupBy() throws Exception {
    Path p = createInputFile(FILE,
        "2138,1\n",
        "2138,2\n",
        "2138,3\n",
        "2138,4\n",
        "2140,1\n",
        "2140,2\n",
        "2140,3\n",
        "2140,5\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToList.class.getName()));

    client.execute(FROM_TABLE + " SELECT col1, f(col2) GROUP BY col1");
    assertEquals("2138\t[1,2,3,4]", client.fetchOne());
    assertEquals("2140\t[1,2,3,5]", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }

  @Test
  public void testGroupConcat() throws Exception {
    Path p = createInputFile(FILE,
        "2138,1\n",
        "2138,2\n",
        "2138,3\n",
        "2138,4\n",
        "2139,1\n",
        "2139,2\n",
        "2139,3\n",
        "2139,5\n",
        "2140,1\n",
        "2140,2\n",
        "2140,3\n",
        "2140,5\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToList.class.getName()));

    client.execute(
        FROM_TABLE
        + " SELECT col1, concat_ws('', f(cast(col2 AS STRING)))"
        + " GROUP BY col1");
    assertEquals("2138\t1234", client.fetchOne());
    assertEquals("2139\t1235", client.fetchOne());
    assertEquals("2140\t1235", client.fetchOne());

    client.execute(
        FROM_TABLE
        + " SELECT col1, concat_ws('->', f(cast(col2 AS STRING)))"
        + " GROUP BY col1");
    assertEquals("2138\t1->2->3->4", client.fetchOne());
    assertEquals("2139\t1->2->3->5", client.fetchOne());
    assertEquals("2140\t1->2->3->5", client.fetchOne());

    client.execute(DROP_TABLE);

    // clean up.
    getFileSystem().delete(p, false);
  }

  @Test
  public void testNested() throws Exception {
    Path p = createInputFile(FILE,
        "2138,1\n",
        "2138,2\n",
        "2138,3\n",
        "2138,4\n",
        "2139,1\n",
        "2139,2\n",
        "2139,3\n",
        "2139,5\n",
        "2140,1\n",
        "2140,2\n",
        "2140,3\n",
        "2140,5\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToList.class.getName()));

    client.execute(
        "FROM("
            + FROM_TABLE
            + " SELECT col1, concat_ws('', f(cast(col2 AS STRING))) AS s"
            + " GROUP BY col1"
        + ")ss "
        + " SELECT s, count(*)"
        + " GROUP BY s");
    assertEquals("1234\t1", client.fetchOne());
    assertEquals("1235\t2", client.fetchOne());
    
    client.execute(
        "FROM("
            + FROM_TABLE
            + " SELECT col1, concat_ws('', f(cast(col2 AS STRING))) AS s"
            + " GROUP BY col1"
        + ")ss "
        + " SELECT s, count(*) AS count"
        + " GROUP BY s"
        + " DISTRIBUTE BY count SORT BY count DESC");
    assertEquals("1235\t2", client.fetchOne());
    assertEquals("1234\t1", client.fetchOne());
    
    client.execute(DROP_TABLE);

    getFileSystem().delete(p, false);
  }

  @Test
  public void testJoin() throws Exception {
    Path p = createInputFile(FILE,
        "2138,1,10\n",
        "2138,2,10\n",
        "2138,3,10\n",
        "2138,4,10\n",
        "2139,1,20\n",
        "2139,2,20\n",
        "2139,3,20\n",
        "2139,5,20\n",
        "2140,1,10\n",
        "2140,2,20\n",
        "2140,3,30\n",
        "2140,5,40\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT, col3 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToList.class.getName()));

    client.execute(
        FROM_TABLE
        + " SELECT col1"
        + " GROUP BY col1 HAVING concat_ws('', f(cast(col2 AS STRING))) = '1235'");
    assertEquals("2139", client.fetchOne());
    assertEquals("2140", client.fetchOne());

    client.execute(
        " FROM ("
            + FROM_TABLE
            + " SELECT col1 "
            + " GROUP BY col1 HAVING concat_ws('', f(cast(col2 AS STRING))) = '1235'"
        + " )a"
        + " JOIN " + TABLE + " b ON a.col1 = b.col1"
        + " SELECT b.col1, max(b.col3), avg(b.col3)"
        + " GROUP BY b.col1");
    assertEquals("2139\t20\t20.0", client.fetchOne());
    assertEquals("2140\t40\t25.0", client.fetchOne());

    client.execute(DROP_TABLE);

    getFileSystem().delete(p, false);
  }

  @Test
  public void testJoin2() throws Exception {
    Path p = createInputFile(FILE,
        "2138,1,10\n",
        "2138,2,10\n",
        "2138,3,10\n",
        "2138,4,10\n",
        "2139,1,20\n",
        "2139,2,20\n",
        "2139,3,20\n",
        "2139,5,20\n",
        "2140,1,10\n",
        "2140,2,20\n",
        "2140,3,30\n",
        "2140,5,40\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT, col3 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToList.class.getName()));

    client.execute(
        " FROM ("
            + "FROM ("
                + FROM_TABLE
                + " SELECT col1"
                + " GROUP BY col1 HAVING concat_ws('', f(cast(col2 AS STRING))) = '1235'"
            + " )a"
            + " JOIN " + TABLE + " b ON a.col1 = b.col1"
            + " SELECT b.col1, max(b.col3) AS max, avg(b.col3) AS avg"
            + " GROUP BY b.col1"
        + ")c"
        + " SELECT avg(max), avg(avg)");
    assertEquals("30.0\t22.5", client.fetchOne());

    client.execute(DROP_TABLE);

    getFileSystem().delete(p, false);
  }

  @Test
  public void testJoin3() throws Exception {
    Path p = createInputFile(FILE,
        "2138,1,10\n",
        "2138,2,10\n",
        "2138,3,10\n",
        "2138,4,10\n",
        "2139,1,20\n",
        "2139,2,20\n",
        "2139,3,20\n",
        "2139,5,20\n",
        "2140,1,10\n",
        "2140,2,20\n",
        "2140,3,30\n",
        "2140,5,40\n");

    client.execute(DROP_TABLE);
    client.execute(CREATE_TABLE + " (col1 INT, col2 INT, col3 INT)" + ROW_FORMAT_COMMA);
    client.execute(String.format(LOAD_LOCAL_INTO_TABLE, p.toString()));
    client.execute(ADD_THIS_JAR);
    client.execute(String.format(CREATE_FUNCTION_f, GenericUDAFToList.class.getName()));

    client.execute(
        FROM_TABLE
        + " SELECT col1, concat_ws('', f(cast(col2 AS STRING))) AS path"
        + " GROUP BY col1"
        + " HAVING concat_ws('', f(cast(col2 AS STRING))) in ('1234', '1235')");
    assertEquals("2138\t1234", client.fetchOne());
    assertEquals("2139\t1235", client.fetchOne());
    assertEquals("2140\t1235", client.fetchOne());

    client.execute(
        "FROM ("
            + FROM_TABLE
            + " SELECT col1, concat_ws('', f(cast(col2 AS STRING))) AS path"
            + " GROUP BY col1"
            + " HAVING concat_ws('', f(cast(col2 AS STRING))) in ('1234', '1235')"
        + " )a"
        + " JOIN " + TABLE + " b ON a.col1 = b.col1"
        + " SELECT a.path AS path, max(b.col3) AS max, avg(b.col3) AS avg"
        + " GROUP BY b.col1, a.path");
    assertEquals("1234\t10\t10.0", client.fetchOne());
    assertEquals("1235\t20\t20.0", client.fetchOne());
    assertEquals("1235\t40\t25.0", client.fetchOne());

    client.execute(
        "FROM ("
            + "FROM ("
                + FROM_TABLE 
                + " SELECT col1, concat_ws('', f(cast(col2 AS STRING))) AS path"
                + " GROUP BY col1"
                + " HAVING concat_ws('', f(cast(col2 AS STRING))) in ('1234', '1235')"
            + " )a"
            + " JOIN " + TABLE + " b ON a.col1 = b.col1"
            + " SELECT a.path AS path, max(b.col3) AS max, avg(b.col3) AS avg"
            + " GROUP BY b.col1, a.path"
        + ")c"
        + " SELECT path, avg(max), avg(avg)"
        + " GROUP BY path");

    assertEquals("1234\t10.0\t10.0", client.fetchOne());
    assertEquals("1235\t30.0\t22.5", client.fetchOne());

    client.execute(DROP_TABLE);

    getFileSystem().delete(p, false);
  }
}
