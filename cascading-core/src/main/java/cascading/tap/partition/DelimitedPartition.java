/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.util.regex.Pattern;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.util.Util;

/**
 * DelimitedPartition is an implementation of the {@link Partition} interface that allows for simple
 * text delimited paths as partitions.
 * <p/>
 * For example, given the delimiter {@code -} (dash), a partition path will have dashes.
 * <p/>
 * Note the delimiter must not be naturally present in any of the values making up the partition.
 */
public class DelimitedPartition implements Partition
  {
  public static final String PATH_DELIM = "/";

  Fields partitionFields;
  String delimiter = PATH_DELIM;

  transient Pattern pattern;

  public DelimitedPartition( Fields partitionFields, String delimiter )
    {
    this( partitionFields );

    this.delimiter = delimiter;
    }

  public DelimitedPartition( Fields partitionFields )
    {
    if( partitionFields == null )
      throw new IllegalArgumentException( "partitionFields must not be null" );

    if( !partitionFields.isDefined() )
      throw new IllegalArgumentException( "partitionFields must be defined, got: " + partitionFields.printVerbose() );

    this.partitionFields = partitionFields;
    }

  @Override
  public int getPathDepth()
    {
    return partitionFields.size();
    }

  @Override
  public Fields getPartitionFields()
    {
    return partitionFields;
    }

  protected Pattern getPattern()
    {
    if( pattern == null )
      pattern = Pattern.compile( delimiter );

    return pattern;
    }

  @Override
  public void toTuple( String partition, TupleEntry tupleEntry )
    {
    if( partition.startsWith( delimiter ) )
      partition = partition.substring( 1 );

    String[] split = getPattern().split( partition );

    tupleEntry.setCanonicalValues( split );
    }

  @Override
  public String toPartition( TupleEntry tupleEntry )
    {
    return Util.join( tupleEntry.asIterableOf( String.class ), delimiter, true );
    }
  }
