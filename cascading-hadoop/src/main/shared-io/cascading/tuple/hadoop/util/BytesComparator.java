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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import cascading.tuple.Hasher;
import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.io.BufferedInputStream;
import org.apache.hadoop.io.WritableComparator;

/**
 * Class BytesComparator is used to compare arrays of bytes.
 * <p/>
 * Note that BytesComparator implements {@link Hasher}, but for the Hasher interface to be applied during grouping,
 * sorting or joining, it must be set on a {@link cascading.tuple.Fields} instance via
 * {@link cascading.tuple.Fields#setComparator(Comparable, java.util.Comparator)}.
 */
public class BytesComparator implements StreamComparator<BufferedInputStream>, Hasher<byte[]>, Comparator<byte[]>, Serializable
  {
  @Override
  public int compare( byte[] lhs, byte[] rhs )
    {
    if( lhs == rhs )
      return 0;

    return WritableComparator.compareBytes( lhs, 0, lhs.length, rhs, 0, rhs.length );
    }

  @Override
  public int compare( BufferedInputStream lhsStream, BufferedInputStream rhsStream )
    {
    byte[] lhs = lhsStream.getBuffer();
    int lhsPos = lhsStream.getPosition();
    int lhsLen = readLen( lhs, lhsPos );

    lhsStream.skip( lhsLen + 4 );

    byte[] rhs = rhsStream.getBuffer();
    int rhsPos = rhsStream.getPosition();
    int rhsLen = readLen( rhs, rhsPos );

    rhsStream.skip( rhsLen + 4 );

    return WritableComparator.compareBytes( lhs, lhsPos + 4, lhsLen, rhs, rhsPos + 4, rhsLen );
    }

  private int readLen( byte[] buffer, int off )
    {
    return ( ( buffer[ off ] & 0xff ) << 24 ) +
      ( ( buffer[ off + 1 ] & 0xff ) << 16 ) +
      ( ( buffer[ off + 2 ] & 0xff ) << 8 ) +
      ( buffer[ off + 3 ] & 0xff );
    }

  @Override
  public int hashCode( byte[] value )
    {
    return Arrays.hashCode( value );
    }
  }
