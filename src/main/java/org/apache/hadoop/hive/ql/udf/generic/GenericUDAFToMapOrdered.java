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

import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * GenericUDAFToMap.
 * @author Francois Rousseau
 */
@Description(name = "to_map_ordered",
             value = "_FUNC_(col1, col2) - Returns a TreeMap with keys from col1 and values from col2")
public class GenericUDAFToMapOrdered extends GenericUDAFToMap {

  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

    super.getEvaluator(parameters);
    return new GenericUDAFToMapOrderedEvaluator();
  }

  public static class GenericUDAFToMapOrderedEvaluator extends GenericUDAFToMapEvaluator {

    public void reset(AggregationBuffer agg) throws HiveException {
      ((ToMapAgg) agg).container = new TreeMap<Object,Object>();
    }

  }
}
