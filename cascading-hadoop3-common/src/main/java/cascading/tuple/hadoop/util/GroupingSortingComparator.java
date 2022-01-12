/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import cascading.tuple.io.TuplePair;
import org.apache.hadoop.conf.Configuration;

/** Class TuplePairComparator is an implementation of {@link org.apache.hadoop.io.RawComparator}. */
public class GroupingSortingComparator extends DeserializerComparator<TuplePair>
  {
  Class[] sortTypes;
  Comparator[] sortComparators;

  @Override
  public void setConf( Configuration conf )
    {
    super.setConf( conf );

    if( conf == null )
      return;

    sortTypes = tupleSerialization.getSortTypes();

    sortComparators = deserializeComparatorsFor( "cascading.sort.comparator" );
    sortComparators = delegatingComparatorsFor( sortTypes, sortComparators );
    }

  public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 )
    {
    try
      {
      lhsBuffer.reset( b1, s1, l1 );
      rhsBuffer.reset( b2, s2, l2 );

      int c = compareTuples( keyTypes, groupComparators );

      if( c != 0 )
        return c;

      return compareTuples( sortTypes, sortComparators );
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

  public int compare( TuplePair lhs, TuplePair rhs )
    {
    int c = compareTuples( groupComparators, lhs.getLhs(), rhs.getLhs() );

    if( c != 0 )
      return c;

    return compareTuples( sortComparators, lhs.getRhs(), rhs.getRhs() );
    }
  }