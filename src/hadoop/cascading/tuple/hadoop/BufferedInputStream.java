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

import java.io.ByteArrayInputStream;

/**
 *
 */
public class BufferedInputStream extends ByteArrayInputStream
  {
  private static final byte[] ZERO_BYTES = new byte[]{};

  public BufferedInputStream()
    {
    super( ZERO_BYTES );
    }

  public void reset( byte[] input, int start, int length )
    {
    this.buf = input;
    this.count = start + length;
    this.mark = start;
    this.pos = start;
    }

  public byte[] getBuffer()
    {
    return buf;
    }

  public int getPosition()
    {
    return pos;
    }

  public int getLength()
    {
    return count;
    }

  public void clear()
    {
    this.buf = ZERO_BYTES;
    this.count = 0;
    this.mark = 0;
    this.pos = 0;
    }
  }

