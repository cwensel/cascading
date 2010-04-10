/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.io.IOException;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.Fields;
import cascading.tuple.TupleInputStream;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.InputBuffer;
import org.apache.hadoop.io.RawComparator;

/** Class DeserializerComparator is the base class for all Cascading comparator classes. */
public abstract class DeserializerComparator<T> extends Configured implements RawComparator<T>
  {
  InputBuffer lhsBuffer = new InputBuffer();
  InputBuffer rhsBuffer = new InputBuffer();

  TupleInputStream lhsStream;
  TupleInputStream rhsStream;

  Comparator[] groupComparators = null;

  @Override
  public void setConf( Configuration conf )
    {
    super.setConf( conf );

    TupleSerialization tupleSerialization = new TupleSerialization( conf );

    lhsStream = new TupleInputStream( lhsBuffer, tupleSerialization.getElementReader() );
    rhsStream = new TupleInputStream( rhsBuffer, tupleSerialization.getElementReader() );

    if( conf == null )
      return;

    groupComparators = deserializeComparatorsFor( "cascading.group.comparator" );
    }

  Comparator[] deserializeComparatorsFor( String name )
    {
    try
      {
      String value = getConf().get( name );

      if( value == null )
        return null;

      return ( (Fields) Util.deserializeBase64( value ) ).getComparators();
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to deserialize comparators for: " + name );
      }
    }

  int compareTuples( Comparator[] comparators ) throws IOException
    {
    int lhsLen = lhsStream.getNumElements();
    int rhsLen = rhsStream.getNumElements();

    int c = lhsLen - rhsLen;

    if( c != 0 )
      return c;

    if( comparators == null )
      comparators = new Comparator[lhsLen];

    for( int i = 0; i < lhsLen; i++ )
      {
      try
        {
        Object lhs = lhsStream.getNextElement();
        Object rhs = rhsStream.getNextElement();

        c = 0;

        if( comparators[ i ] != null )
          c = comparators[ i ].compare( lhs, rhs );
        else if( lhs == null && rhs == null )
          c = 0;
        else if( lhs == null && rhs != null )
            return -1;
          else if( lhs != null && rhs == null )
              return 1;
            else
              c = ( (Comparable) lhs ).compareTo( (Comparable) rhs ); // guaranteed to not be null

        if( c != 0 )
          return c;

        }
      catch( Exception exception )
        {
        throw new CascadingException( "unable to compare Tuples, likely a CoGroup is being attempted on fields of " +
          "different types or custom comparators are incorrectly set on Fields", exception );
        }

      if( c != 0 )
        return c;
      }

    return 0;
    }
  }