/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * NamedPartition is an implementation of the {@link Partition} interface that allows for simple
 * key value pair delimited paths as partitions. For example 'year=2020/month=12/day=1/session=foo'.
 * <p>
 * Note neither the keyValueDelimiter or partDelimiter must not be naturally present in any of the values making up the
 * partition.
 * <p>
 * The postfix value will be appended to any partition when created, and removed when the partition is parsed. Use
 * this value to add static filenames to the output path. It is safe to include the delimiter in the postfix value
 * (e.g '/somepath/filename.csv' where the delimiter is the default '/').
 * <p>
 * Note some platforms do not allow for referencing files directly on write, only allowing for partitioning into
 * directories where the actual filename is generated. In this case, if the postfix is intended to be a filename, it
 * will be interpreted as a directory.
 */
public class NamedPartition extends DelimitedPartition
  {
  public static final String KEY_VALUE_DELIM = "=";
  protected String keyValueDelimiter = KEY_VALUE_DELIM;
  protected boolean printNull = true;

  private transient Map<String, Integer> posMap;
  private transient Pattern keyValuePattern;

  public NamedPartition( Fields partitionFields )
    {
    super( partitionFields );
    }

  public NamedPartition( Fields partitionFields, String partDelimiter )
    {
    super( partitionFields, partDelimiter );
    }

  public NamedPartition( Fields partitionFields, String partDelimiter, String keyValueDelimiter )
    {
    super( partitionFields, partDelimiter );
    this.keyValueDelimiter = keyValueDelimiter;
    }

  public NamedPartition( Fields partitionFields, String partDelimiter, String keyValueDelimiter, String postfix )
    {
    super( partitionFields, partDelimiter, postfix );
    this.keyValueDelimiter = keyValueDelimiter;
    }

  public NamedPartition( Fields partitionFields, boolean printNull )
    {
    super( partitionFields );
    this.printNull = printNull;
    }

  public NamedPartition( Fields partitionFields, String partDelimiter, boolean printNull )
    {
    super( partitionFields, partDelimiter );
    this.printNull = printNull;
    }

  public NamedPartition( Fields partitionFields, String partDelimiter, String keyValueDelimiter, boolean printNull )
    {
    super( partitionFields, partDelimiter );
    this.keyValueDelimiter = keyValueDelimiter;
    this.printNull = printNull;
    }

  public NamedPartition( Fields partitionFields, String partDelimiter, String keyValueDelimiter, String postfix, boolean printNull )
    {
    super( partitionFields, partDelimiter, postfix );
    this.keyValueDelimiter = keyValueDelimiter;
    this.printNull = printNull;
    }

  protected Pattern getKeyValuePattern()
    {
    if( keyValuePattern == null )
      keyValuePattern = Pattern.compile( keyValueDelimiter );

    return keyValuePattern;
    }

  private Map<String, Integer> getPosMap()
    {
    if( posMap != null )
      return posMap;

    posMap = new HashMap<>();

    partitionFields.forEach( f -> posMap.put( String.valueOf( f ), partitionFields.getPos( f ) ) );

    return posMap;
    }

  protected void parsePartitionInto( String partition, Fields partitionFields, int numSplits, TupleEntry tupleEntry )
    {
    Map<String, Integer> posMap = getPosMap();
    String[] split = getPattern().split( partition, numSplits );

    for( String entry : split )
      {
      String[] keyValue = getKeyValuePattern().split( entry, 2 );

      tupleEntry.setString( posMap.get( keyValue[ 0 ] ), keyValue[ 1 ] );
      }
    }

  protected String formatPartitionWith( TupleEntry tupleEntry, String delimiter )
    {
    Iterable<String[]> iterable = tupleEntry.asPairwiseIterable();

    int count = 0;

    StringBuilder buffer = new StringBuilder();

    for( String[] s : iterable )
      {
      if( count != 0 )
        buffer.append( delimiter );

      buffer.append( s[ 0 ] );
      buffer.append( keyValueDelimiter );

      if( printNull || s[ 1 ] != null )
        buffer.append( s[ 1 ] );

      count++;
      }

    return buffer.toString();
    }
  }
