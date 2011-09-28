/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.io.IOException;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.flow.hadoop.HadoopUtil;
import cascading.tuple.Fields;
import cascading.tuple.StreamComparator;
import cascading.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;

/** Class DeserializerComparator is the base class for all Cascading comparator classes. */
public abstract class DeserializerComparator<T> extends Configured implements RawComparator<T>
  {
  final BufferedInputStream lhsBuffer = new BufferedInputStream();
  final BufferedInputStream rhsBuffer = new BufferedInputStream();

  TupleSerialization tupleSerialization;

  HadoopTupleInputStream lhsStream;
  HadoopTupleInputStream rhsStream;

  Comparator[] groupComparators;

  @Override
  public void setConf( Configuration conf )
    {
    if( conf == null )
      return;

    super.setConf( conf );

    tupleSerialization = new TupleSerialization( conf );

    // get new readers so deserializers don't compete for the buffer
    lhsStream = new HadoopTupleInputStream( lhsBuffer, tupleSerialization.getElementReader() );
    rhsStream = new HadoopTupleInputStream( rhsBuffer, tupleSerialization.getElementReader() );

    groupComparators = deserializeComparatorsFor( "cascading.group.comparator" );
    groupComparators = delegatingComparatorsFor( groupComparators );
    }

  Comparator[] deserializeComparatorsFor( String name )
    {
    try
      {
      if( getConf() == null )
        throw new IllegalStateException( "no conf set" );

      String value = getConf().get( name );

      if( value == null )
        return new Comparator[ getConf().getInt( name + ".size", 1 ) ];

      Fields fields = (Fields) HadoopUtil.deserializeBase64( value );

      return fields.getComparators();
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to deserialize comparators for: " + name );
      }
    }

  Comparator[] delegatingComparatorsFor( Comparator[] fieldComparators )
    {
    Comparator[] comparators = new Comparator[ fieldComparators.length ];

    for( int i = 0; i < comparators.length; i++ )
      {
      if( fieldComparators[ i ] instanceof StreamComparator )
        comparators[ i ] = new TupleElementStreamComparator( (StreamComparator) fieldComparators[ i ] );
      else if( fieldComparators[ i ] != null )
        comparators[ i ] = new TupleElementComparator( fieldComparators[ i ] );
      else
        comparators[ i ] = new DelegatingTupleElementComparator( tupleSerialization );
      }

    return comparators;
    }

  final int compareTuples( Comparator[] comparators, Tuple lhs, Tuple rhs )
    {
    int lhsLen = lhs.size();
    int rhsLen = rhs.size();

    int c = lhsLen - rhsLen;

    if( c != 0 )
      return c;

    for( int i = 0; i < lhsLen; i++ )
      {
      // hack to support comparators array length of 1
      c = comparators[ i % comparators.length ].compare( lhs.getObject( i ), rhs.getObject( i ) );

      if( c != 0 )
        return c;
      }

    return 0;
    }

  final int compareTuples( Comparator[] comparators ) throws IOException
    {
    int lhsLen = lhsStream.getNumElements();
    int rhsLen = rhsStream.getNumElements();

    int c = lhsLen - rhsLen;

    if( c != 0 )
      return c;

    for( int i = 0; i < lhsLen; i++ )
      {
      // hack to support comparators array length of 1
      c = ( (StreamComparator) comparators[ i % comparators.length ] ).compare( lhsStream, rhsStream );

      if( c != 0 )
        return c;
      }

    return 0;
    }
  }