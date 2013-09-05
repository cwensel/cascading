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

import java.util.regex.Pattern;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.util.Util;

/**
 *
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
