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

package cascading.platform.hadoop;

import java.io.Serializable;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.Hasher;
import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.BufferedInputStream;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;

/**
 *
 */
public class TestLongComparator implements Hasher<Long>, StreamComparator<BufferedInputStream>, Comparator<Long>, Serializable
  {
  boolean reverse = true;

  public TestLongComparator()
    {
    }

  public TestLongComparator( boolean reverse )
    {
    this.reverse = reverse;
    }

  @Override
  public int compare( Long lhs, Long rhs )
    {
    if( lhs == null && rhs == null )
      return 0;

    if( lhs == null )
      return !reverse ? -1 : 1;

    if( rhs == null )
      return !reverse ? 1 : -1;

    return reverse ? rhs.compareTo( lhs ) : lhs.compareTo( rhs );
    }

  @Override
  public int compare( BufferedInputStream lhsStream, BufferedInputStream rhsStream )
    {
    if( lhsStream == null && rhsStream == null )
      return 0;

    if( lhsStream == null )
      return !reverse ? -1 : 1;

    if( rhsStream == null )
      return !reverse ? 1 : -1;

    HadoopTupleInputStream lhsInput = new HadoopTupleInputStream( lhsStream, new TupleSerialization().getElementReader() );
    HadoopTupleInputStream rhsInput = new HadoopTupleInputStream( rhsStream, new TupleSerialization().getElementReader() );

    try
      {
      // explicit for debugging purposes
      Long l1 = (Long) lhsInput.readVLong();
      Long l2 = (Long) rhsInput.readVLong();

      return reverse ? l2.compareTo( l1 ) : l1.compareTo( l2 );
      }
    catch( Exception exception )
      {
      throw new CascadingException( exception );
      }
    }

  @Override
  public int hashCode( Long value )
    {
    if( value == null )
      return 0;
    return value.hashCode();
    }
  }
