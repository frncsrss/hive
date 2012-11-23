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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFToList.
 * @author Francois Rousseau
 */
@Description(name = "to_list", value = "_FUNC_(x) - Returns a list from all the values of x."
             + "CAUTION will easily OOM on large data sets.")
public class GenericUDAFToList extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFToList.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument are expected.");
    }

    final TypeInfo parameter = parameters[0];
    if (parameter.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameter.getTypeName() + " is passed.");
    }

    return new GenericUDAFToListEvaluator();
  }

  /**
   * GenericUDAFToListEvaluator.
   *
   */
  public static class GenericUDAFToListEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private PrimitiveObjectInspector inputOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations 
    private StandardListObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      if (m == Mode.PARTIAL1) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        return ObjectInspectorFactory.getStandardListObjectInspector(
            (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI));
      } else {
        if (!(parameters[0] instanceof StandardListObjectInspector)) {
          inputOI = (PrimitiveObjectInspector) parameters[0];
          return ObjectInspectorFactory.getStandardListObjectInspector(
              (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI));
        } else {
          internalMergeOI = (StandardListObjectInspector) parameters[0];
          inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
          return (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(
              internalMergeOI);                 
        }
      }
    }

    /** class for storing the current string builder. */
    static class ToListAgg implements AggregationBuffer {
      List<Object> container;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((ToListAgg) agg).container = new ArrayList<Object>();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      ToListAgg result = new ToListAgg();
      reset(result);
      return result;
    }

    protected void putIntoList(Object p, ToListAgg myagg) {
      Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,this.inputOI);
      myagg.container.add(pCopy);
    }

    boolean warned = false;

    // Map-side
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        Object p = parameters[0];
        if (p != null) {
          ToListAgg myagg = (ToListAgg) agg;
          putIntoList(p, myagg);
        }
      } catch (NumberFormatException e) {
        if (!warned) {
          warned = true;
          LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
          LOG.warn(getClass().getSimpleName() + " ignoring similar exceptions.");
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
      ToListAgg myagg = (ToListAgg) agg;
      @SuppressWarnings("unchecked")
      ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
      for(Object i : partialResult) {
        putIntoList(i, myagg);
      }
    }

    // Reduce-side
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      ToListAgg myagg = (ToListAgg) agg;
      if (myagg.container.size() < 0) {
        return null;
      }
      return myagg.container;
    }

  }

}
