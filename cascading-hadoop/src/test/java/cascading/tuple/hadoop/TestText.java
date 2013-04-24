/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
  public String toString()
    {
    return value;
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
