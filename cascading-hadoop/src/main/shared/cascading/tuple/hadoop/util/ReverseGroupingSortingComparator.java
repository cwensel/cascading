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

import cascading.tuple.io.TuplePair;

/** Class ReverseTuplePairComparator is an implementation of {@link org.apache.hadoop.io.RawComparator}. */
public class ReverseGroupingSortingComparator extends GroupingSortingComparator
  {
  @Override
  public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 )
    {
    return -1 * super.compare( b1, s1, l1, b2, s2, l2 );
    }

  @Override
  public int compare( TuplePair lhs, TuplePair rhs )
    {
    return super.compare( rhs, lhs );
    }
  }