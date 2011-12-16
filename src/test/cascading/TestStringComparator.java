/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import cascading.tuple.Hasher;
import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.BufferedInputStream;
import cascading.tuple.hadoop.HadoopTupleInputStream;
import cascading.tuple.hadoop.TupleSerialization;

/**
 *
 */
public class TestStringComparator implements Hasher<String>, StreamComparator<BufferedInputStream>, Comparator<String>, Serializable
  {
  boolean reverse = true;

  public TestStringComparator()
    {
    }

  public TestStringComparator( boolean reverse )
    {
    this.reverse = reverse;
    }

  @Override
  public int compare( String o1, String o2 )
    {
    return reverse ? o2.compareTo( o1 ) : o1.compareTo( o2 );
    }

  @Override
  public int compare( BufferedInputStream lhsStream, BufferedInputStream rhsStream )
    {
    HadoopTupleInputStream lhsInput = new HadoopTupleInputStream( lhsStream, new TupleSerialization().getElementReader() );
    HadoopTupleInputStream rhsInput = new HadoopTupleInputStream( rhsStream, new TupleSerialization().getElementReader() );

    try
      {
      // explicit for debugging purposes
      String s1 = (String) lhsInput.readString();
      String s2 = (String) rhsInput.readString();
      return reverse ? s2.compareTo( s1 ) : s1.compareTo( s2 );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    }

  @Override
  public int hashCode( String value )
    {
    return value.hashCode();
    }
  }
