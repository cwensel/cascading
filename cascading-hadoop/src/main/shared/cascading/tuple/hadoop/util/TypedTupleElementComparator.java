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

package cascading.tuple.hadoop.util;

import java.io.IOException;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.StreamComparator;
import cascading.tuple.io.TupleInputStream;

/**
 *
 */
public class TypedTupleElementComparator implements StreamComparator<TupleInputStream>, Comparator<Object>
  {
  private Class type;

  Comparator comparator = new Comparator<Comparable>()
  {
  @Override
  public int compare( Comparable lhs, Comparable rhs )
    {
    if( lhs == null && rhs == null )
      return 0;

    if( lhs == null )
      return -1;

    if( rhs == null )
      return 1;

    return lhs.compareTo( rhs ); // guaranteed to not be null
    }
  };

  public TypedTupleElementComparator()
    {
    }

  public TypedTupleElementComparator( Class type, Comparator comparator )
    {
    this.type = type;

    if( comparator != null )
      this.comparator = comparator;
    }

  @Override
  public int compare( Object lhs, Object rhs )
    {
    return comparator.compare( lhs, rhs );
    }

  @Override
  public int compare( TupleInputStream lhsStream, TupleInputStream rhsStream )
    {
    Object lhs;
    Object rhs;

    try
      {
      lhs = lhsStream.readType( type );
      rhs = rhsStream.readType( type );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to read element from underlying stream", exception );
      }

    try
      {
      return comparator.compare( lhs, rhs );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to compare Tuples, likely a CoGroup is being attempted on fields of " +
        "different types or custom comparators are incorrectly set on Fields, lhs: '" + lhs + "' rhs: '" + rhs + "'", exception );
      }
    }
  }
