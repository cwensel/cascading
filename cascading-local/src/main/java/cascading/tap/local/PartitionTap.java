/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.tap.local;

import java.beans.ConstructorProperties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.flow.local.LocalFlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.io.TapFileOutputStream;
import cascading.tap.partition.BasePartitionTap;
import cascading.tap.partition.Partition;
import cascading.tap.type.FileType;
import cascading.tap.type.TapWith;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

/**
 * Class PartitionTap can be used to write tuple streams out to files and sub-directories based on the values in the
 * current {@link cascading.tuple.Tuple} instance.
 * <p>
 * The constructor takes a {@link cascading.tap.local.FileTap} {@link cascading.tap.Tap} and a {@link Partition}
 * implementation. This allows Tuple values at given positions to be used as directory names during write
 * operations, and directory names as data during read operations.
 * <p>
 * The key value here is that there is no need to duplicate data values in the directory names and inside
 * the data files.
 * <p>
 * So only values declared in the parent Tap will be read or written to the underlying file system files. But
 * fields declared by the {@link Partition} will only be read or written to the directory names. That is, the
 * PartitionTap instance will sink or source the partition fields, plus the parent Tap fields. The partition
 * fields and parent Tap fields do not need to have common field names.
 * <p>
 * {@code openWritesThreshold} limits the number of open files to be output to. This value defaults to 300 files.
 * Each time the threshold is exceeded, 10% of the least recently used open files will be closed.
 * <p>
 * PartitionTap will populate a given {@code partition} without regard to case of the values being used. Thus
 * the resulting paths {@code 2012/June/} and {@code 2012/june/} will likely result in two open files into the same
 * location. Forcing the case to be consistent with a custom Partition implementation or an upstream
 * {@link cascading.operation.Function} is recommended, see {@link cascading.operation.expression.ExpressionFunction}.
 */
@SuppressWarnings("JavadocReference")
public class PartitionTap extends BasePartitionTap<Properties, InputStream, OutputStream>
  {
  public static final String PART_NUM_PROPERTY = "cascading.local.tap.partition.seq";

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.local.FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   *
   * @param parent    of type Tap
   * @param partition of type Partition
   */
  @ConstructorProperties({"parent", "partition"})
  public PartitionTap( Tap parent, Partition partition )
    {
    this( parent, partition, OPEN_WRITES_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.local.FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   * <p>
   * {@code openWritesThreshold} limits the number of open files to be output to.
   *
   * @param parent              of type Hfs
   * @param partition           of type Partition
   * @param openWritesThreshold of type int
   */
  @ConstructorProperties({"parent", "partition", "openWritesThreshold"})
  public PartitionTap( Tap parent, Partition partition, int openWritesThreshold )
    {
    super( parent, partition, openWritesThreshold );
    }

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.local.FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   *
   * @param parent    of type Tap
   * @param partition of type Partition
   * @param sinkMode  of type SinkMode
   */
  @ConstructorProperties({"parent", "partition", "sinkMode"})
  public PartitionTap( Tap parent, Partition partition, SinkMode sinkMode )
    {
    super( parent, partition, sinkMode );
    }

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.local.FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   * <p>
   * {@code keepParentOnDelete}, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(Object)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   *
   * @param parent             of type Tap
   * @param partition          of type Partition
   * @param sinkMode           of type SinkMode
   * @param keepParentOnDelete of type boolean
   */
  @ConstructorProperties({"parent", "partition", "sinkMode", "keepParentOnDelete"})
  public PartitionTap( Tap parent, Partition partition, SinkMode sinkMode, boolean keepParentOnDelete )
    {
    this( parent, partition, sinkMode, keepParentOnDelete, OPEN_WRITES_THRESHOLD_DEFAULT );
    }

  /**
   * Constructor PartitionTap creates a new PartitionTap instance using the given parent {@link cascading.tap.local.FileTap} Tap as the
   * base path and default {@link cascading.scheme.Scheme}, and the partition.
   * <p>
   * {@code keepParentOnDelete}, when set to true, prevents the parent Tap from being deleted when {@link #deleteResource(Object)}
   * is called, typically an issue when used inside a {@link cascading.cascade.Cascade}.
   * <p>
   * {@code openWritesThreshold} limits the number of open files to be output to.
   *
   * @param parent              of type Tap
   * @param partition           of type Partition
   * @param sinkMode            of type SinkMode
   * @param keepParentOnDelete  of type boolean
   * @param openWritesThreshold of type int
   */
  @ConstructorProperties({"parent", "partition", "sinkMode", "keepParentOnDelete", "openWritesThreshold"})
  public PartitionTap( Tap parent, Partition partition, SinkMode sinkMode, boolean keepParentOnDelete, int openWritesThreshold )
    {
    super( parent, partition, sinkMode, keepParentOnDelete, openWritesThreshold );

    if( !( parent instanceof FileType ) )
      throw new IllegalArgumentException( "parent Tap must be of type: " + FileType.class.getName() );
    }

  @Override
  protected String getCurrentIdentifier( FlowProcess<? extends Properties> flowProcess )
    {
    return null;
    }

  @Override
  public boolean deleteResource( Properties conf ) throws IOException
    {
    String[] childIdentifiers = ( (FileTap) parent ).getChildIdentifiers( conf, Integer.MAX_VALUE, false );

    if( childIdentifiers.length == 0 )
      return deleteParent( conf );

    DirTap.deleteChildren( Paths.get( parent.getIdentifier() ), childIdentifiers );

    return deleteParent( conf );
    }

  private boolean deleteParent( Properties conf ) throws IOException
    {
    return keepParentOnDelete || parent.deleteResource( conf );
    }

  @Override
  protected TupleEntrySchemeCollector createTupleEntrySchemeCollector( FlowProcess<? extends Properties> flowProcess, Tap parent, String path, long sequence ) throws IOException
    {
    if( sequence != -1 && flowProcess.getConfig() != null )
      ( (LocalFlowProcess) FlowProcessWrapper.undelegate( flowProcess ) ).getConfig().setProperty( PART_NUM_PROPERTY, Long.toString( sequence ) );

    if( parent instanceof TapWith )
      return (TupleEntrySchemeCollector) ( (TapWith) parent )
        .withChildIdentifier( path )
        .withSinkMode( SinkMode.UPDATE )
        .asTap().openForWrite( flowProcess );

    TapFileOutputStream output = new TapFileOutputStream( parent, path, true ); // always append

    return new TupleEntrySchemeCollector<Properties, OutputStream>( flowProcess, parent, output );
    }

  @Override
  protected TupleEntrySchemeIterator createTupleEntrySchemeIterator( FlowProcess<? extends Properties> flowProcess, Tap parent, String path, InputStream input ) throws IOException
    {
    if( parent instanceof TapWith )
      return (TupleEntrySchemeIterator) ( (TapWith) parent )
        .withChildIdentifier( path )
        .asTap().openForRead( flowProcess, input );

    if( input == null )
      input = new FileInputStream( path );

    return new TupleEntrySchemeIterator( flowProcess, parent, parent.getScheme(), input, path );
    }
  }
