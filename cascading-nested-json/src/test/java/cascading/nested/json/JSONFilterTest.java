/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.nested.json;

import java.util.regex.Pattern;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

/**
 *
 */
public class JSONFilterTest extends CascadingTestCase
  {
  @Test
  public void testRegexFilter() throws Exception
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.nested );

    assertTrue( invokeFilter( new JSONRegexFilter( "/person/name", Pattern.compile( "John D Doe" ) ), entry ) );
    assertTrue( invokeFilter( new JSONRegexFilter( "/person/name", Pattern.compile( "^John$" ) ), entry ) );

    assertFalse( invokeFilter( new JSONRegexFilter( "/person/name", Pattern.compile( "^John Doe$" ) ), entry ) );
    assertFalse( invokeFilter( new JSONRegexFilter( "/person/name", Pattern.compile( "John Doe" ) ), entry ) );
    assertFalse( invokeFilter( new JSONRegexFilter( "/person/name", Pattern.compile( "John[ ]Doe$" ) ), entry ) );
    }
  }
