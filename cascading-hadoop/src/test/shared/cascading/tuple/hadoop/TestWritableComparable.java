/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

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
