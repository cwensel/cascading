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
import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.io.TupleInputStream;

/**
 *
 */
public class DelegatingTupleElementComparator implements StreamComparator<TupleInputStream>, Comparator<Object>
  {
  final TupleSerialization tupleSerialization;
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

    Comparator comparator = tupleSerialization.getComparator( type );

    if( comparator instanceof StreamComparator )
      return new TupleElementStreamComparator( (StreamComparator) comparator );

    return new TupleElementComparator( comparator );
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

      // grab the configured default comparator, its ok if its null, just wasn't configured externally
      if( foundComparator == null )
        foundComparator = tupleSerialization.getDefaultComparator();

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