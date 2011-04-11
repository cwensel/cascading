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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.StreamComparator;
import org.apache.hadoop.io.WritableUtils;

/** Class BytesComparator is used to compare arrays of bytes. */
public class TestTextComparator implements StreamComparator<BufferedInputStream>, Comparator<TestText>, Serializable
  {
  @Override
  public int compare( TestText lhs, TestText rhs )
    {
    if( lhs == rhs )
      return 0;

    if( lhs == null )
      return -1;

    if( rhs == null )
      return 1;

    if( lhs.value == null && rhs.value == null )
      return 0;

    if( lhs.value == null )
      return -1;

    if( rhs.value == null )
      return 1;

    return lhs.value.compareTo( rhs.value );
    }

  @Override
  public int compare( BufferedInputStream lhsStream, BufferedInputStream rhsStream )
    {
    try
      {
      if( lhsStream == null && rhsStream == null )
        return 0;

      if( lhsStream == null )
        {
        WritableUtils.readString( new DataInputStream( rhsStream ) );
        return -1;
        }

      if( rhsStream == null )
        {
        WritableUtils.readString( new DataInputStream( lhsStream ) );
        return 1;
        }

      String lhsString = WritableUtils.readString( new DataInputStream( lhsStream ) );
      String rhsString = WritableUtils.readString( new DataInputStream( rhsStream ) );

      if( lhsString == null && rhsString == null )
        return 0;

      if( lhsString == null )
        return -1;

      if( rhsString == null )
        return 1;

      return lhsString.compareTo( rhsString );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    }
  }