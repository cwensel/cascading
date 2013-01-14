/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.collect;

import java.util.Collection;

import cascading.provider.CascadingFactory;
import cascading.tuple.Tuple;

/**
 * Interface TupleCollectionFactory allows developers to plugin alternative implementations of a "tuple collection"
 * used to back in memory "join" and "co-group" operations. Typically these implementations are
 * "spillable", in that to prevent using up all memory in the JVM, after some threshold is met or event
 * is triggered, values are persisted to disk.
 * <p/>
 * The {@link java.util.Collection} classes returned must take a {@link cascading.tuple.Tuple} as a value.
 * <p/>
 * If the Collection implementation implements the {@link Spillable} interface, it will receive a {@link Spillable.SpillListener}
 * instance that calls back to the appropriate logging mechanism for the platform.
 * <p/>
 * The class {@link SpillableTupleList} may be used as a base class.
 *
 * @see cascading.tuple.hadoop.collect.HadoopTupleCollectionFactory
 * @see SpillableTupleList
 * @see cascading.tuple.hadoop.collect.HadoopSpillableTupleList
 * @see TupleMapFactory
 * @see cascading.tuple.hadoop.collect.HadoopTupleMapFactory
 */
public interface TupleCollectionFactory<Config> extends CascadingFactory<Config, Collection<Tuple>>
  {
  String TUPLE_COLLECTION_FACTORY = "cascading.factory.tuple.collection.classname";
  }