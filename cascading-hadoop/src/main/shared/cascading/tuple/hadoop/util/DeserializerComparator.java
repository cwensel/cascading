/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop.util;

import java.io.IOException;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tuple.Fields;
import cascading.tuple.StreamComparator;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.BufferedInputStream;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;

/** Class DeserializerComparator is the base class for all Cascading comparator classes. */
public abstract class DeserializerComparator<T> extends Configured implements RawComparator<T>
  {
  protected BufferedInputStream lhsBuffer;
  protected BufferedInputStream rhsBuffer;

  protected TupleSerialization tupleSerialization;

  protected HadoopTupleInputStream lhsStream;
  protected HadoopTupleInputStream rhsStream;

  protected Class[] keyTypes;
  protected Comparator[] groupComparators;

  boolean hasConfiguredComparators = false; //

  protected boolean canPerformRawComparisons()
    {
    return false;
    }

  protected boolean performRawComparison()
    {
    return canPerformRawComparisons() && keyTypes != null && !hasConfiguredComparators;
    }

  @Override
  public void setConf( Configuration conf )
    {
    if( conf == null )
      return;

    super.setConf( conf );

    tupleSerialization = new TupleSerialization( conf );

    keyTypes = tupleSerialization.getKeyTypes();

    groupComparators = deserializeComparatorsFor( "cascading.group.comparator" );
    groupComparators = delegatingComparatorsFor( keyTypes, groupComparators );

    if( performRawComparison() )
      return;

    lhsBuffer = new BufferedInputStream();
    rhsBuffer = new BufferedInputStream();

    // get new readers so deserializers don't compete for the buffer
    lhsStream = getHadoopTupleInputStream( lhsBuffer, tupleSerialization.getElementReader() );
    rhsStream = getHadoopTupleInputStream( rhsBuffer, tupleSerialization.getElementReader() );
    }

  protected HadoopTupleInputStream getHadoopTupleInputStream( BufferedInputStream lhsBuffer, TupleSerialization.SerializationElementReader elementReader )
    {
    return new HadoopTupleInputStream( lhsBuffer, elementReader );
    }

  Comparator[] deserializeComparatorsFor( String name )
    {
    Configuration conf = getConf();

    if( conf == null )
      throw new IllegalStateException( "no conf set" );

    return getFieldComparatorsFrom( conf, name );
    }

  public static Comparator[] getFieldComparatorsFrom( Configuration conf, String name )
    {
    String value = conf.get( name );

    if( value == null )
      return new Comparator[ conf.getInt( name + ".size", 1 ) ];

    try
      {
      return HadoopUtil.deserializeBase64( value, conf, Fields.class ).getComparators();
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to deserialize comparators for: " + name );
      }
    }

  Comparator[] delegatingComparatorsFor( Class[] types, Comparator[] fieldComparators )
    {
    Comparator[] comparators = new Comparator[ fieldComparators.length ];

    for( int i = 0; i < comparators.length; i++ )
      {
      if( types != null )
        {
        Class type = types[ i ];

        if( fieldComparators[ i ] != null )
          comparators[ i ] = fieldComparators[ i ]; // provided on selector
        else
          comparators[ i ] = tupleSerialization.getComparator( type ); // provided via Serialization or conf, may return null

        if( comparators[ i ] != null )
          hasConfiguredComparators = true;

        if( comparators[ i ] instanceof StreamComparator )
          comparators[ i ] = new TypedTupleElementStreamComparator( type, (StreamComparator) comparators[ i ] );
        else
          comparators[ i ] = new TypedTupleElementComparator( type, comparators[ i ] );
        }
      else
        {
        if( fieldComparators[ i ] instanceof StreamComparator )
          comparators[ i ] = new TupleElementStreamComparator( (StreamComparator) fieldComparators[ i ] );
        else if( fieldComparators[ i ] != null )
          comparators[ i ] = new TupleElementComparator( fieldComparators[ i ] );
        else
          comparators[ i ] = new DelegatingTupleElementComparator( tupleSerialization ); // lazy lookup
        }
      }

    return comparators;
    }

  protected final int compareTuples( Comparator[] comparators, Tuple lhs, Tuple rhs )
    {
    int lhsLen = lhs.size();
    int rhsLen = rhs.size();

    int c = lhsLen - rhsLen;

    if( c != 0 )
      return c;

    for( int i = 0; i < lhsLen; i++ )
      {
      // hack to support comparators array length of 1
      Object lhsObject = lhs.getObject( i );
      Object rhsObject = rhs.getObject( i );

      try
        {
        c = comparators[ i % comparators.length ].compare( lhsObject, rhsObject );
        }
      catch( Exception exception )
        {
        throw new CascadingException( "unable to compare object elements in position: " + i + " lhs: '" + lhsObject + "' rhs: '" + rhsObject + "'", exception );
        }

      if( c != 0 )
        return c;
      }

    return 0;
    }

  protected final int compareTuples( Class[] types, Comparator[] comparators ) throws IOException
    {
    if( types == null )
      return compareUnTypedTuples( comparators );
    else
      return compareTypedTuples( types, comparators );
    }

  final int compareTypedTuples( Class[] types, Comparator[] comparators ) throws IOException
    {
    int c;

    for( int i = 0; i < types.length; i++ )
      {
      try
        {
        c = ( (StreamComparator) comparators[ i % comparators.length ] ).compare( lhsStream, rhsStream );
        }
      catch( Exception exception )
        {
        throw new CascadingException( "unable to compare stream elements in position: " + i, exception );
        }

      if( c != 0 )
        return c;
      }

    return 0;
    }

  final int compareUnTypedTuples( Comparator[] comparators ) throws IOException
    {
    int lhsLen = lhsStream.getNumElements();
    int rhsLen = rhsStream.getNumElements();

    int c = lhsLen - rhsLen;

    if( c != 0 )
      return c;

    for( int i = 0; i < lhsLen; i++ )
      {
      try
        {
        c = ( (StreamComparator) comparators[ i % comparators.length ] ).compare( lhsStream, rhsStream );
        }
      catch( Exception exception )
        {
        throw new CascadingException( "unable to compare stream elements in position: " + i, exception );
        }

      if( c != 0 )
        return c;
      }

    return 0;
    }
  }