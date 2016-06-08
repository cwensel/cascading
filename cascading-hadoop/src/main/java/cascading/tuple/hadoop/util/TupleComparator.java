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

public class TupleComparator extends BaseTupleComparator
  {
  static RawComparison BYTE_COMPARISON = new ByteComparison();

  @Override
  protected RawComparison getByteComparison()
    {
    return BYTE_COMPARISON;
    }

  static class ByteComparison implements RawComparison
    {
    @Override
    public int compare( byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2 )
      {
      if( buffer1 == buffer2 && offset1 == offset2 && length1 == length2 )
        return 0;

      int end1 = offset1 + length1;
      int end2 = offset2 + length2;

      for( int i = offset1, j = offset2; i < end1 && j < end2; i++, j++ )
        {
        int a = ( buffer1[ i ] & 0xff );
        int b = ( buffer2[ j ] & 0xff );

        if( a != b )
          return a - b;
        }

      return length1 - length2;
      }
    }
  }