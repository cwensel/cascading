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

import java.io.InputStream;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;

/**
 *
 */
public class TupleElementStreamComparator implements StreamComparator<HadoopTupleInputStream>, Comparator<Object>
  {
  final StreamComparator comparator;

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
  public int compare( HadoopTupleInputStream lhsStream, HadoopTupleInputStream rhsStream )
    {
    try
      {
      // pop off element type, its assumed we know it as we have a stream comparator
      // to delegate too
      int lhsToken = lhsStream.readToken();
      int rhsToken = rhsStream.readToken();

      if( lhsToken == HadoopTupleOutputStream.WRITABLE_TOKEN )
        lhsStream.readString();

      if( rhsToken == HadoopTupleOutputStream.WRITABLE_TOKEN )
        rhsStream.readString();

      InputStream lhs = lhsToken == 0 ? null : lhsStream.getInputStream();
      InputStream rhs = rhsToken == 0 ? null : rhsStream.getInputStream();

      return comparator.compare( lhs, rhs );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to compare Tuples, likely a CoGroup is being attempted on fields of " +
        "different types or custom comparators are incorrectly set on Fields", exception );
      }
    }
  }