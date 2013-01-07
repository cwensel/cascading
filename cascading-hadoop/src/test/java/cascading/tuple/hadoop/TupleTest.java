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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import cascading.CascadingTestCase;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;

public class TupleTest extends CascadingTestCase
  {
  public TupleTest()
    {
    }

  public void testWritableCompareReadWrite() throws IOException
    {
    Tuple aTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "3.0", true );

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    TupleOutputStream dataOutputStream = new HadoopTupleOutputStream( byteArrayOutputStream, new TupleSerialization().getElementWriter() );

    dataOutputStream.writeTuple( aTuple );

    dataOutputStream.flush();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
    TupleInputStream dataInputStream = new HadoopTupleInputStream( byteArrayInputStream, new TupleSerialization().getElementReader() );

    Tuple newTuple = new Tuple();

    dataInputStream.readTuple( newTuple );

    assertEquals( aTuple, newTuple );
    }

  public void testWritableCompare()
    {
    Tuple aTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "3.0" );
    Tuple bTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "3.0" );

    assertEquals( "not equal: aTuple", bTuple, aTuple );
    assertTrue( "not equal than: aTuple = bTuple", aTuple.compareTo( bTuple ) == 0 );

    bTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "2.0" );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) > 0 );
    }
  }
