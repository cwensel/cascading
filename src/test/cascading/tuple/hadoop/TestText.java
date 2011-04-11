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

/**
 *
 */
public class TestText implements Comparable<TestText>
  {
  String value;

  public TestText()
    {
    }

  public TestText( String string )
    {
    this.value = string;
    }

  @Override
  public int compareTo( TestText o )
    {
    if( value == null )
      return -1;

    if( o == null )
      return 1;

    return value.compareTo( o.value );
    }

  @Override
  public int hashCode()
    {
    if( value == null )
      return 0;

    return value.hashCode();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    TestText testText = (TestText) object;

    if( value != null ? !value.equals( testText.value ) : testText.value != null )
      return false;

    return true;
    }
  }
