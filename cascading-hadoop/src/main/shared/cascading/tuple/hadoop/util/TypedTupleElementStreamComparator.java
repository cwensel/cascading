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

import java.io.InputStream;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;

/**
 *
 */
public class TypedTupleElementStreamComparator implements StreamComparator<HadoopTupleInputStream>, Comparator<Object>
  {
  private final Class type;
  final StreamComparator comparator;

  public TypedTupleElementStreamComparator( Class type, StreamComparator comparator )
    {
    this.type = type;
    this.comparator = comparator;
    }

  @Override
  public int compare( Object lhs, Object rhs )
    {
    return ( (Comparator<Object>) comparator ).compare( lhs, rhs );
    }

  @Override
  public int compare( HadoopTupleInputStream lhsStream, HadoopTupleInputStream rhsStream )
    {
    try
      {
      InputStream lhs = lhsStream.getInputStream();
      InputStream rhs = rhsStream.getInputStream();

      if( !type.isPrimitive() )
        {
        int lhsToken = lhsStream.readToken();
        int rhsToken = rhsStream.readToken();

        lhs = lhsToken == 0 ? null : lhs;
        rhs = rhsToken == 0 ? null : rhs;
        }

      return comparator.compare( lhs, rhs );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to compare Tuples, likely a CoGroup is being attempted on fields of " +
        "different types or custom comparators are incorrectly set on Fields", exception );
      }
    }
  }