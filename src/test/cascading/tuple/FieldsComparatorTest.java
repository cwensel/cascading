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

package cascading.tuple;

import java.util.Comparator;

import cascading.CascadingTestCase;
import cascading.TestStringComparator;
import cascading.test.PlatformTest;

/**
 *
 */
@PlatformTest(platforms = {"none"})
public class FieldsComparatorTest extends CascadingTestCase
  {
  Comparator comparator = new TestStringComparator( false );

  public FieldsComparatorTest()
    {
    super();
    }

  public void testCompare()
    {
    Fields fields = new Fields( "a" );

    fields.setComparator( "a", comparator );

    Tuple aTuple = new Tuple( "a" );
    Tuple bTuple = new Tuple( "b" );

    assertTrue( "not less than: aTuple < bTuple", fields.compare( aTuple, bTuple ) < 0 );
    assertTrue( "not less than: bTuple < aTuple", fields.compare( bTuple, aTuple ) > 0 );

    aTuple.add( "b" );

    assertTrue( "not greater than: aTuple > bTuple", fields.compare( aTuple, bTuple ) > 0 );

    aTuple = new Tuple( bTuple, "a" );

    assertTrue( "not greater than: aTuple > bTuple", fields.compare( aTuple, bTuple ) > 0 );
    }

  public void testCompare2()
    {
    Fields fields = new Fields( "a" );

    fields.setComparators( new Comparator[]{comparator} );

    Tuple aTuple = new Tuple( "a" );
    Tuple bTuple = new Tuple( "b" );

    assertTrue( "not less than: aTuple < bTuple", fields.compare( aTuple, bTuple ) < 0 );
    assertTrue( "not less than: bTuple < aTuple", fields.compare( bTuple, aTuple ) > 0 );

    aTuple.add( "b" );

    assertTrue( "not greater than: aTuple > bTuple", fields.compare( aTuple, bTuple ) > 0 );

    aTuple = new Tuple( bTuple, "a" );

    assertTrue( "not greater than: aTuple > bTuple", fields.compare( aTuple, bTuple ) > 0 );
    }
  }
