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

package cascading.tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 */
public class TestWritableComparable implements WritableComparable
  {
  private String value;

  public TestWritableComparable()
    {
    }

  public TestWritableComparable( String value )
    {
    this.value = value;
    }

  public void write( DataOutput dataOutput ) throws IOException
    {
    WritableUtils.writeString( dataOutput, value );
    }

  public void readFields( DataInput dataInput ) throws IOException
    {
    value = WritableUtils.readString( dataInput );
    }

  public int compareTo( Object object )
    {
    return value.compareTo( object.toString() );
    }

  public String toString()
    {
    return value;
    }

  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    TestWritableComparable that = (TestWritableComparable) object;

    if( value != null ? !value.equals( that.value ) : that.value != null )
      return false;

    return true;
    }

  public int hashCode()
    {
    return ( value != null ? value.hashCode() : 0 );
    }
  }
