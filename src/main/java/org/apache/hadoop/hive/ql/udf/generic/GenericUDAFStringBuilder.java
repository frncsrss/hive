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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFStringBuilder.
 * @author Francois Rousseau
 */
@Description(name = "string_builder",
             value = "_FUNC_(x) - Returns a concatenated string from all the values inside x"
                 + "CAUTION will easily OOM on large data sets.")

public class GenericUDAFStringBuilder extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFStringBuilder.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }

    final TypeInfo parameter = parameters[0];
    if (parameter.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameter.getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo) parameter).getPrimitiveCategory()) {
      case SHORT:
      case INT:
      case LONG:
      case STRING:
        break;
      case TIMESTAMP:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      default:
        throw new UDFArgumentTypeException(0,
            "Only integer or string type arguments are accepted but "
            + parameters[0].getTypeName() + " is passed.");
    }
    return new GenericUDAFStringBuilderEvaluator();
  }

  /**
   * GenericUDAFStringBuilderEvaluator.
   *
   */
  public static class GenericUDAFStringBuilderEvaluator extends GenericUDAFEvaluator {
    private PrimitiveObjectInspector inputOI;
    private Text result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      result = new Text();
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    /** class for storing the current string builder. */
    static class StringBuilderAgg implements AggregationBuffer {
      StringBuilder container;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((StringBuilderAgg) agg).container = new StringBuilder();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      StringBuilderAgg result = new StringBuilderAgg();
      reset(result);
      return result;
    }

    boolean warned = false;

    // Map-side
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        merge(agg, parameters[0]);
      } catch (NumberFormatException e) {
        if (!warned) {
          warned = true;
          LOG.warn(getClass().getSimpleName() + " "
              + StringUtils.stringifyException(e));
          LOG.warn(getClass().getSimpleName()
              + " ignoring similar exceptions.");
        }
      }
    }

    // Map-side
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }

    // Reduce-side
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        String pCopy = PrimitiveObjectInspectorUtils.getString(partial, inputOI);
        ((StringBuilderAgg) agg).container.append(pCopy);
      }
    }

    // Reduce-side
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StringBuilderAgg myagg = (StringBuilderAgg) agg;
      if (myagg.container.length() == 0) {
        return null;
      }
      result.set(myagg.container.toString());
      return result;
    }

  }

}
