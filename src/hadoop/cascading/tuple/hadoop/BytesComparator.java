/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.Comparator;

import cascading.tuple.StreamComparator;
import org.apache.hadoop.io.WritableComparator;

/** Class BytesComparator is used to compare arrays of bytes. */
public class BytesComparator implements StreamComparator<BufferedInputStream>, Comparator<byte[]>, Serializable
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
  }
