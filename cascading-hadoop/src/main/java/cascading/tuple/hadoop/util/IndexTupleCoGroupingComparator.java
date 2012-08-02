/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingException;
import cascading.tuple.io.IndexTuple;
import org.apache.hadoop.conf.Configurable;

public class IndexTupleCoGroupingComparator extends DeserializerComparator<IndexTuple> implements Configurable
  {
  public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 )
    {
    try
      {
      lhsBuffer.reset( b1, s1, l1 );
      rhsBuffer.reset( b2, s2, l2 );

      int lhsIndex = lhsStream.readVInt();
      int rhsIndex = rhsStream.readVInt();

      int c = compareTuples( groupComparators );

      if( c != 0 )
        return c;

      return rhsIndex - lhsIndex;
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    finally
      {
      lhsBuffer.clear();
      rhsBuffer.clear();
      }
    }

  public int compare( IndexTuple lhs, IndexTuple rhs )
    {
    int c = compareTuples( groupComparators, lhs.getTuple(), rhs.getTuple() );

    if( c != 0 )
      return c;

    // intentionally sort in reverse
    return rhs.getIndex() - lhs.getIndex();
    }
  }