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

import java.io.InputStream;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.StreamComparator;
import cascading.tuple.TupleInputStream;
import cascading.tuple.TupleOutputStream;

/**
 *
 */
public class TupleElementStreamComparator implements StreamComparator<TupleInputStream>, Comparator<Object>
  {
  StreamComparator comparator;

  public TupleElementStreamComparator( StreamComparator comparator )
    {
    this.comparator = comparator;
    }

  @Override
  public int compare( Object lhs, Object rhs )
    {
    return ( (Comparator<Object>) comparator ).compare( lhs, rhs );
    }

  @Override
  public int compare( TupleInputStream lhsStream, TupleInputStream rhsStream )
    {
    try
      {
      // pop off element type, its assumed we know it as we have a stream comparator
      // to delegate too
      int lhsToken = lhsStream.readToken();
      rhsStream.readToken();

      if( lhsToken == TupleOutputStream.WRITABLE_TOKEN )
        {
        lhsStream.readString();
        rhsStream.readString();
        }

      InputStream lhs = (InputStream) lhsStream.getInputStream();
      InputStream rhs = (InputStream) rhsStream.getInputStream();

      return comparator.compare( lhs, rhs );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to compare Tuples, likely a CoGroup is being attempted on fields of " +
        "different types or custom comparators are incorrectly set on Fields", exception );
      }
    }
  }