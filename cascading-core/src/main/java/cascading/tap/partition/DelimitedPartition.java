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

package cascading.tap.partition;

import java.util.regex.Pattern;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.util.Util;

/**
 * DelimitedPartition is an implementation of the {@link Partition} interface that allows for simple
 * text delimited paths as partitions.
 * <p>
 * For example, given the delimiter {@code -} (dash), a partition path will have dashes.
 * <p>
 * Note the delimiter must not be naturally present in any of the values making up the partition.
 * <p>
 * The postfix value will be appended to any partition when created, and removed when the partition is parsed. Use
 * this value to add static filenames to the output path. It is safe to include the delimiter in the postfix value
 * (e.g '/somepath/filename.csv' where the delimiter is the default '/').
 * <p>
 * Note some platforms do not allow for referencing files directly on write, only allowing for partitioning into
 * directories where the actual filename is generated. In this case, if the postfix is intended to be a filename, it
 * will be interpreted as a directory.
 */
public class DelimitedPartition implements Partition
  {
  public static final String PATH_DELIM = "/";

  final Fields partitionFields;
  final String delimiter;
  final String postfix;

  int numSplits;

  transient Pattern pattern;

  public DelimitedPartition( Fields partitionFields )
    {
    this( partitionFields, null, null );
    }

  public DelimitedPartition( Fields partitionFields, String delimiter )
    {
    this( partitionFields, delimiter, null );
    }

  public DelimitedPartition( Fields partitionFields, String delimiter, String postfix )
    {
    if( partitionFields == null )
      throw new IllegalArgumentException( "partitionFields must not be null" );

    if( !partitionFields.isDefined() )
      throw new IllegalArgumentException( "partitionFields must be defined, got: " + partitionFields.printVerbose() );

    this.partitionFields = partitionFields;
    this.delimiter = delimiter == null ? PATH_DELIM : delimiter;

    postfix = Util.isEmpty( postfix ) ? null : postfix.startsWith( this.delimiter ) ? postfix.substring( this.delimiter.length() ) : postfix;

    this.numSplits = partitionFields.size() + ( postfix != null ? postfix.split( this.delimiter ).length : 0 );
    this.postfix = postfix == null ? null : delimiter + postfix; // prefix the postfix w/ the delimiter
    }

  @Override
  public int getPathDepth()
    {
    return numSplits;
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

  public String getDelimiter()
    {
    return delimiter;
    }

  public String getPostfix()
    {
    return postfix;
    }

  @Override
  public void toTuple( String partition, TupleEntry tupleEntry )
    {
    if( partition.startsWith( delimiter ) )
      partition = partition.substring( 1 );

    parsePartitionInto( partition, partitionFields, numSplits, tupleEntry );
    }

  protected void parsePartitionInto( String partition, Fields partitionFields, int numSplits, TupleEntry tupleEntry )
    {
    String[] split = getPattern().split( partition, numSplits );

    tupleEntry.setCanonicalValues( split, 0, partitionFields.size() );
    }

  @Override
  public String toPartition( TupleEntry tupleEntry )
    {
    String partition = formatPartitionWith( tupleEntry, delimiter );

    if( postfix != null )
      partition = partition + postfix; // delimiter prefixed in ctor

    return partition;
    }

  protected String formatPartitionWith( TupleEntry tupleEntry, String delimiter )
    {
    return Util.join( tupleEntry.asIterableOf( String.class ), delimiter, true );
    }
  }
