/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tap.hadoop.io.MultiInputSplit;
import cascading.tap.hadoop.io.TapOutputCollector;
import cascading.tap.partition.BasePartitionTap;
import cascading.tap.partition.Partition;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Class PartitionTap can be used to write tuple streams out to files and sub-directories based on the values in the
 * current {@link cascading.tuple.Tuple} instance.
 * <p/>
 * The constructor takes a {@link cascading.tap.hadoop.Hfs} {@link cascading.tap.Tap} and a {@link Partition}
 * implementation. This allows Tuple values at given positions to be used as directory names during write
 * operations, and directory names as data during read operations.
 * <p/>
 * The key value here is that there is no need to duplicate data values in the directory names and inside
 * the data files.
 * <p/>
 * So only values declared in the parent Tap will be read or written to the underlying file system files. But
 * fields declared by the {@link Partition} will only be read or written to the directory names. That is, the
 * PartitionTap instance will sink or source the partition fields, plus the parent Tap fields. The partition
 * fields and parent Tap fields do not need to have common field names.
 * <p/>
 * Note that Hadoop can only sink to directories, and all files in those directories are "part-xxxxx" files.
 * <p/>
 * {@code openWritesThreshold} limits the number of open files to be output to. This value defaults to 300 files.
 * Each time the threshold is exceeded, 10% of the least recently used open files will be closed.
 * <p/>
 * PartitionTap will populate a given {@code partition} without regard to case of the values being used. Thus
 * the resulting paths {@code 2012/June/} and {@code 2012/june/} will likely result in two open files into the same
 * location. Forcing the case to be consistent with a custom Partition implementation or an upstream
 * {@link cascading.operation.Function} is recommended, see {@link cascading.operation.expression.ExpressionFunction}.
 * <p/>
 * Though Hadoop has no mechanism to prevent simultaneous writes to a directory from multiple jobs, it doesn't mean
 * its safe to do so. Same is true with the PartitionTap. Interleaving writes to a common parent (root) directory
 * across multiple flows will very likely lead to data loss.
 */
public class PartitionTap extends BasePartitionTap<Configuration, RecordReader, OutputCollector>
  {
  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.hadoop.Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   *
   * @param parent    of type Tap
   * @param partition of type String
   */
  @ConstructorProperties({"parent", "partition"})
  public PartitionTap( Hfs parent, Partition partition )
    {
    this( parent, partition, OPEN_WRITES_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.hadoop.Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   * <p/>
   * {@code openWritesThreshold} limits the number of open files to be output to.
   *
   * @param parent              of type Hfs
   * @param partition           of type String
   * @param openWritesThreshold of type int
   */
  @ConstructorProperties({"parent", "partition", "openWritesThreshold"})
  public PartitionTap( Hfs parent, Partition partition, int openWritesThreshold )
    {
    super( parent, partition, openWritesThreshold );
    }

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.hadoop.Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   *
   * @param parent    of type Tap
   * @param partition of type String
   * @param sinkMode  of type SinkMode
   */
  @ConstructorProperties({"parent", "partition", "sinkMode"})
  public PartitionTap( Hfs parent, Partition partition, SinkMode sinkMode )
    {
    super( parent, partition, sinkMode );
    }

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.hadoop.Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   * <p/>
   * {@code keepParentOnDelete}, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(Object)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   *
   * @param parent             of type Tap
   * @param partition          of type String
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   */
  @ConstructorProperties({"parent", "partition", "sinkMode", "keepParentOnDelete"})
  public PartitionTap( Hfs parent, Partition partition, SinkMode sinkMode, boolean keepParentOnDelete )
    {
    this( parent, partition, sinkMode, keepParentOnDelete, OPEN_WRITES_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.hadoop.Hfs} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   * <p/>
   * {@code keepParentOnDelete}, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(Object)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   * <p/>
   * {@code openWritesThreshold} limits the number of open files to be output to.
   *
   * @param parent              of type Tap
   * @param partition           of type String
   * @param sinkMode            of type SinkMode
   * @param keepParentOnDelete  of type boolean
   * @param openWritesThreshold of type int
   */
  @ConstructorProperties({"parent", "partition", "sinkMode", "keepParentOnDelete", "openWritesThreshold"})
  public PartitionTap( Hfs parent, Partition partition, SinkMode sinkMode, boolean keepParentOnDelete, int openWritesThreshold )
    {
    super( parent, partition, sinkMode, keepParentOnDelete, openWritesThreshold );
    }

  @Override
  protected TupleEntrySchemeCollector createTupleEntrySchemeCollector( FlowProcess<? extends Configuration> flowProcess, Tap parent, String path, long sequence ) throws IOException
    {
    TapOutputCollector outputCollector = new TapOutputCollector( flowProcess, parent, path, sequence );

    return new TupleEntrySchemeCollector<Configuration, OutputCollector>( flowProcess, parent, outputCollector );
    }

  @Override
  protected TupleEntrySchemeIterator createTupleEntrySchemeIterator( FlowProcess<? extends Configuration> flowProcess, Tap parent, String path, RecordReader recordReader ) throws IOException
    {
    return new HadoopTupleEntrySchemeIterator( flowProcess, new Hfs( parent.getScheme(), path ), recordReader );
    }

  @Override
  protected String getCurrentIdentifier( FlowProcess<? extends Configuration> flowProcess )
    {
    String identifier = flowProcess.getStringProperty( MultiInputSplit.CASCADING_SOURCE_PATH ); // set on current split

    if( identifier == null )
      return null;

    return new Path( identifier ).getParent().toString(); // drop part-xxxx
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> flowProcess, Configuration conf )
    {
    try
      {
      String[] childPartitions = getChildPartitionIdentifiers( flowProcess, true );

      ( (Hfs) getParent() ).applySourceConfInitIdentifiers( flowProcess, conf, childPartitions );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to retrieve child partitions", exception );
      }
    }
  }
