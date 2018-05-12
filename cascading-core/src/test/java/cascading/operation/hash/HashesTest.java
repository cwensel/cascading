/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.operation.hash;

import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;
import org.junit.Test;

/**
 *
 */
public class HashesTest extends CascadingTestCase
  {

  @Test
  public void testBase64URL()
    {
    Fields hash = new Fields( "hash", String.class );

    Base64URLHashFunction function = new Base64URLHashFunction( hash, pre -> "foo" + pre, post -> post.insert( 0, "prefix-" ) );

    Tuple[] arguments = new Tuple[]{
      new Tuple( "foo" ),
      new Tuple( "foo" ),
      new Tuple( "foo", "bar" ),
      };

    TupleListCollector tuples = invokeFunction( function, arguments, hash );

//    tuples.forEach( System.out::println );

    Iterator<Tuple> iterator = tuples.iterator();

    Tuple first = iterator.next();
    assertEquals( first, iterator.next() );
    assertNotSame( first, iterator.next() );
    }

  @Test
  public void testBase10()
    {
    Fields hash = new Fields( "hash", String.class );
    Base10HashFunction function = new Base10HashFunction( hash );

    Tuple[] arguments = new Tuple[]{
      new Tuple( "foo" ),
      new Tuple( "foo" ),
      new Tuple( "foo", "bar" ),
      };

    TupleListCollector tuples = invokeFunction( function, arguments, hash );

//    tuples.forEach( System.out::println );

    Iterator<Tuple> iterator = tuples.iterator();

    Tuple first = iterator.next();
    assertEquals( first, iterator.next() );
    assertNotSame( first, iterator.next() );
    }
  }
