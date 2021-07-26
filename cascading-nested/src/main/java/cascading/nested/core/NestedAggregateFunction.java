/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.nested.core;

import java.io.Serializable;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.type.CoercibleType;

/**
 * Interface NestedAggregateFunction provides a basic abstraction for aggregating set of values collected from
 * a set of parent objects.
 *
 * @see NestedGetAllAggregateFunction
 */
public interface NestedAggregateFunction<Node, Context> extends Serializable
  {
  /**
   * @return the fields this aggregate function will return
   */
  Fields getFieldDeclaration();

  /**
   * @return a new function specific context object
   */
  Context createContext();

  /**
   * This method receives each collected value to be applied to the current aggregation.
   *
   * @param context       the context object created by {@link #createContext()}
   * @param coercibleType the {@link CoercibleType} used to manage the given Node
   * @param node          the Node container object that should be aggregated
   */
  void aggregate( Context context, CoercibleType<Node> coercibleType, Node node );

  /**
   * This method completes the aggregate operation and insert the results into a Tuple instance.
   * <p>
   * Note the Tuple may be created (indirectly) by the {@link #createContext()} method and reused.
   *
   * @param context the context object created by {@link #createContext()}
   * @return a Tuple containing the aggregate value(s), the Tuple may be reused
   */
  Tuple complete( Context context );

  /**
   * This method is called after {@link #complete(Object)} so that any resources can be freed and the result
   * context instance can be used in the next aggregation.
   *
   * @param context the context object created by {@link #createContext()}
   * @return either a cleared or new context instance to be used in further aggregations
   */
  Context resetContext( Context context );
  }
