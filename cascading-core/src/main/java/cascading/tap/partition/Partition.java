/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
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