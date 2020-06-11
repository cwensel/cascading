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

package cascading.tap.partition;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

/**
 *
 */
public class PartitionTest extends CascadingTestCase
  {
  @Test
  public void namedPartition()
    {
    String expected = "a=foo/b=100/c=false/d=";

    Fields partitionFields = new Fields( "a", String.class )
      .append( new Fields( "b", Integer.TYPE ) )
      .append( new Fields( "c", Boolean.class ) )
      .append( new Fields( "d", Long.class ) );

    NamedPartition partition = new NamedPartition( partitionFields, "/", false );

    TupleEntry entry = new TupleEntry( partitionFields );

    Tuple tuple = new Tuple( "foo", 100, false, null );
    entry.setTuple( tuple );

    assertEquals( expected, partition.toPartition( entry ) );

    entry.setTuple( Tuple.size( entry.size() ) );
    partition.toTuple( expected, entry );

    assertEquals( tuple, entry.getTuple() );
    }
  }
