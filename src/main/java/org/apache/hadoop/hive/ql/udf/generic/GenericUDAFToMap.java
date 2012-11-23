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
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFToMap.
 * @author Francois Rousseau
 */
@Description(name = "to_map",
             value = "_FUNC_(col1, col2) - Returns a HashMap with keys from col1 and values from col2")
public class GenericUDAFToMap extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFToMap.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly two arguments are expected.");
    }

    final TypeInfo parameter = parameters[0];
    if (parameter.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameter.getTypeName() + " is passed.");
    }

    return new GenericUDAFToMapEvaluator();
  }

  /**
   * GenericUDAFToMapEvaluator.
   *
   */
  public static class GenericUDAFToMapEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private PrimitiveObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations 
    private StandardMapObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      if (m == Mode.PARTIAL1) {
        inputKeyOI = (PrimitiveObjectInspector) parameters[0];
        inputValueOI = (ObjectInspector) parameters[1];
        return ObjectInspectorFactory.getStandardMapObjectInspector(
            (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI), 
            (ObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputValueOI));
      } else {
        if (!(parameters[0] instanceof StandardMapObjectInspector)) {
          inputKeyOI = (PrimitiveObjectInspector) parameters[0];
          inputValueOI = (ObjectInspector) parameters[1];
          return ObjectInspectorFactory.getStandardMapObjectInspector(
              (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI), 
              (ObjectInspector) PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        } else {
            internalMergeOI = (StandardMapObjectInspector) parameters[0];
            inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
            inputValueOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
            return (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(
                internalMergeOI);                 
        }
      }
    }

    /** class for storing the current string builder. */
    static class ToMapAgg implements AggregationBuffer {
      Map<Object,Object> container;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((ToMapAgg) agg).container = new HashMap<Object,Object>(144);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      ToMapAgg result = new ToMapAgg();
      reset(result);
      return result;
    }

    protected void putIntoMap(Object key, Object value, ToMapAgg myagg) {
      Object pKeyCopy = ObjectInspectorUtils.copyToStandardObject(key, inputKeyOI);
      Object pValueCopy = ObjectInspectorUtils.copyToStandardObject(value, inputValueOI);
      myagg.container.put(pKeyCopy, pValueCopy);
    }

    boolean warned = false;

    // Map-side
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 2);
      try {
        Object key = parameters[0];
        Object value = parameters[1];
        if (key != null) {
          ToMapAgg myagg = (ToMapAgg) agg;
          putIntoMap(key, value, myagg);
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
      if (partial != null) {
        ToMapAgg myagg = (ToMapAgg) agg;
        @SuppressWarnings("unchecked")
        Map<Object,Object> partialResult = (Map<Object,Object>) internalMergeOI.getMap(partial);
        for (Map.Entry<Object,Object> entry: partialResult.entrySet()) {
          putIntoMap(entry.getKey(), entry.getValue(), myagg);
        }
      }
    }

    // Reduce-side
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      ToMapAgg myagg = (ToMapAgg) agg;
      if (myagg.container.size() < 0) {
        return null;
      }
      return myagg.container;
    }

  }

}
