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

package cascading.tuple.hadoop;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.io.BufferedInputStream;
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