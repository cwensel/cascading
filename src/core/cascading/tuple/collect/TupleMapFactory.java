/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
import java.util.Map;

import cascading.provider.CascadingFactory;
import cascading.tuple.Tuple;

/**
 * Interface TupleMapFactory allows developers to plugin alternative implementations of a "tuple map"
 * used to back in memory "join" and "co-group" operations. Typically these implementations are
 * "spillable", in that to prevent using up all memory in the JVM, after some threshold is met or event
 * is triggered, values are persisted to disk.
 * <p/>
 * The {@link Map} classes returned must take a {@link cascading.tuple.Tuple} as a key, and a {@link Collection} of Tuples as
 * a value. Further, {@link Map#get(Object)} must never return {@code null}, but on the first call to get() on the map
 * an empty Collection must be created and stored.
 * <p/>
 * That is, {@link Map#put(Object, Object)} is never called on the map instance internally,
 * only {@code map.get(groupTuple).add(valuesTuple)}.
 * <p/>
 * Using the {@link TupleCollectionFactory} to create the underlying Tuple Collections would allow that aspect
 * to be pluggable as well.
 * <p/>
 * If the Map implementation implements the {@link Spillable} interface, it will receive a {@link Spillable.SpillListener}
 * instance that calls back to the appropriate logging mechanism for the platform. This instance should be passed
 * down to any child Spillable types, namely an implementation of {@link SpillableTupleList}.
 * <p/>
 * The default implementation for the Hadoop platform is the {@link cascading.tuple.hadoop.collect.HadoopTupleMapFactory}
 * which created a {@link cascading.tuple.hadoop.collect.HadoopSpillableTupleMap} instance.
 * <p/>
 * The class {@link SpillableTupleMap} may be used as a base class.
 *
 * @see SpillableTupleMap
 * @see cascading.tuple.hadoop.collect.HadoopTupleMapFactory
 * @see TupleCollectionFactory
 * @see cascading.tuple.hadoop.collect.HadoopTupleCollectionFactory
 */
public interface TupleMapFactory extends CascadingFactory<Map<Tuple, Collection<Tuple>>>
  {
  String TUPLE_MAP_FACTORY = "cascading.factory.tuple.map.classname";
  }