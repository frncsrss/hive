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

import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFCounterMap.
 * @author Francois Rousseau
 */
@Description(name = "counter_map",
             value = "_FUNC_(x) - Returns a counter map from all the values inside x")
public class GenericUDAFCounterMap extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFCounterMap.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
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

    return new GenericUDAFCounterMapEvaluator();
  }

  /**
   * GenericUDAFCounterMapEvaluator.
   *
   */
  public static class GenericUDAFCounterMapEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private PrimitiveObjectInspector inputOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations 
    private StandardMapObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      if (m == Mode.PARTIAL1) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        return ObjectInspectorFactory.getStandardMapObjectInspector(
            (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI), 
            (ObjectInspector) PrimitiveObjectInspectorFactory.writableLongObjectInspector);
      } else {
        if (!(parameters[0] instanceof StandardMapObjectInspector)) {
          inputOI = (PrimitiveObjectInspector) parameters[0];
          return ObjectInspectorFactory.getStandardMapObjectInspector(
              (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI), 
              (ObjectInspector) PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        } else {
            internalMergeOI = (StandardMapObjectInspector) parameters[0];
            inputOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
            return (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(
                internalMergeOI);                 
        }
      }
    }

    /** class for storing the current string builder. */
    static class CounterMapAgg implements AggregationBuffer {
      Map<Object,LongWritable> container;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((CounterMapAgg) agg).container = new HashMap<Object,LongWritable>();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      CounterMapAgg result = new CounterMapAgg();
      reset(result);
      return result;
    }

    protected void putIntoMap(Object key, LongWritable value, CounterMapAgg myagg) {
      Object pKeyCopy = ObjectInspectorUtils.copyToStandardObject(key, inputOI);
      if(myagg.container.containsKey(pKeyCopy)) {
        myagg.container.get(pKeyCopy).set(value.get() + myagg.container.get(pKeyCopy).get());
      } else {
        myagg.container.put(pKeyCopy, new LongWritable(value.get()));          
      }
    }

    protected void putOneIntoMap(Object key, CounterMapAgg myagg) {
      Object pKeyCopy = ObjectInspectorUtils.copyToStandardObject(key, inputOI);
      if(myagg.container.containsKey(pKeyCopy)) {
        myagg.container.get(pKeyCopy).set(1L + myagg.container.get(pKeyCopy).get());
      } else {
        myagg.container.put(pKeyCopy, new LongWritable(1L));
      }
    }

    boolean warned = false;

    // Map-side
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 1);
      try {
        CounterMapAgg myagg = (CounterMapAgg) agg;
        putOneIntoMap(parameters[0], myagg);
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
      CounterMapAgg myagg = (CounterMapAgg) agg;
      if (myagg.container.size() < 0) {
        return null;
      }
      return myagg.container;
    }

    // Reduce-side
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        CounterMapAgg myagg = (CounterMapAgg) agg;
        @SuppressWarnings("unchecked")
        Map<Object,LongWritable> partialResult =
            (Map<Object,LongWritable>) internalMergeOI.getMap(partial);
        for (Map.Entry<Object,LongWritable> entry: partialResult.entrySet()) {
          putIntoMap(entry.getKey(), entry.getValue(), myagg);
        }
      }
    }

    // Reduce-side
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      CounterMapAgg myagg = (CounterMapAgg) agg;
      if (myagg.container.size() < 0) {
        return null;
      }
      return myagg.container;
    }

  }

}
