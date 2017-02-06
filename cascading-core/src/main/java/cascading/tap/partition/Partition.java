/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tap.partition;

import java.io.Serializable;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * The Partition interface allows for custom partitioning mechanisms to be created with the {@link BasePartitionTap}
 * sub-classes.
 * <p/>
 * A partition is a directory on a filesystem, where the directory contains data related to the files underneath
 * the partition directory.
 * <p/>
 * For example, a partition could be {@code "2012/09/01"}, which would contain log files for that day.
 */
public interface Partition extends Serializable
  {
  /**
   * Returns the directory search depth of the partition.
   * <p/>
   * For example, a Partition implementation that returns values like {@code "2012/09/01"} would have a depth of 3.
   *
   * @return an int
   */
  int getPathDepth();

  /**
   * The {@link Fields} used to populate the partition.
   *
   * @return a Fields instance
   */
  Fields getPartitionFields();

  /**
   * Converts the given partition String to a {@link TupleEntry} using the given TupleEntry instance for re-use.
   *
   * @param partition  a String
   * @param tupleEntry a TupleEntry
   */
  void toTuple( String partition, TupleEntry tupleEntry );

  /**
   * Converts the given tupleEntry into a partition string.
   *
   * @param tupleEntry a TupleEntry
   * @return a String
   */
  String toPartition( TupleEntry tupleEntry );
  }