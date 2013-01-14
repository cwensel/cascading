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

package cascading.tuple;

import java.util.Comparator;

/**
 * The Comparison interface allows specific underlying type mechanisms to additionally return relevant
 * {@link Comparator} implementations when required internally.
 * <p/>
 * In the case of Hadoop, {@link org.apache.hadoop.io.serializer.Serialization} implementations that present
 * alternative serialization implementations for custom types manged by {@link Tuple}s should also
 * implement the {@link #getComparator(Class)} method.
 * <p/>
 * During runtime Cascading can identify and use the correct Comparator during grouping operations if it was
 * not given explicitly on the {@link Fields#setComparator(Comparable, java.util.Comparator)} family of methods.
 * <p/>
 * See the class {@link cascading.tuple.hadoop.BytesSerialization} for an example.
 * <p/>
 * see cascading.tuple.hadoop.BytesSerialization
 */
public interface Comparison<T>
  {
  Comparator<T> getComparator( Class<T> type );
  }