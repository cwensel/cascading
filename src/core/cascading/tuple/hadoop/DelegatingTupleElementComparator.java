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
import cascading.tuple.StreamComparator;
import cascading.tuple.TupleInputStream;

/**
 *
 */
public class DelegatingTupleElementComparator implements StreamComparator<TupleInputStream>, Comparator<Object>
  {
  TupleSerialization tupleSerialization;
  Comparator<Object> objectComparator = null;
  StreamComparator<TupleInputStream> streamComparator = null;

  public DelegatingTupleElementComparator( TupleSerialization tupleSerialization )
    {
    this.tupleSerialization = tupleSerialization;
    }

  @Override
  public int compare( Object lhs, Object rhs )
    {
    if( objectComparator == null )
      {
      if( lhs == null && rhs == null )
        return 0;

      objectComparator = getComparator( lhs, rhs );
      }

    return objectComparator.compare( lhs, rhs );
    }

  private Comparator<Object> getComparator( Object lhs, Object rhs )
    {
    Class type = lhs != null ? lhs.getClass() : null;

    type = type == null && rhs != null ? rhs.getClass() : type;

    return new TupleElementComparator( tupleSerialization.getComparator( type ) );
    }

  @Override
  public int compare( TupleInputStream lhsStream, TupleInputStream rhsStream )
    {
    if( streamComparator == null )
      streamComparator = getComparator( lhsStream );

    return streamComparator.compare( lhsStream, rhsStream );
    }

  private StreamComparator getComparator( TupleInputStream lhsStream )
    {
    try
      {
      lhsStream.mark( 4 * 1024 );

      Comparator foundComparator = lhsStream.getComparatorFor( lhsStream.readToken() );

      if( foundComparator instanceof StreamComparator )
        return new TupleElementStreamComparator( (StreamComparator) foundComparator );
      else
        return new TupleElementComparator( foundComparator );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    finally
      {
      try
        {
        lhsStream.reset();
        }
      catch( IOException exception )
        {
        throw new CascadingException( exception );
        }
      }
    }

  }