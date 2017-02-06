/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.operation.expression;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.filter.And;
import cascading.operation.filter.FilterNotNull;
import cascading.operation.filter.FilterNull;
import cascading.operation.filter.Not;
import cascading.operation.filter.Or;
import cascading.operation.filter.Xor;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

/**
 *
 */
public class FilterTest extends CascadingTestCase
  {
  public FilterTest()
    {
    }

  // these tests verify an Expression can be safely nested.
  @Test
  public void testOrExpression()
    {
    Fields inputFields = new Fields( "a", "b" );
    ExpressionFilter f1 = new ExpressionFilter( "( 100f < a )", new String[]{"a"}, new Class<?>[]{Float.TYPE} );
    ExpressionFilter f2 = new ExpressionFilter( "( 100f < b )", new String[]{"b"}, new Class<?>[]{Float.TYPE} );
    Or logic = new Or( new Fields( "a" ), f1, new Fields( "b" ), f2 );

    boolean[] results = invokeFilter( logic,
      new TupleEntry[]{
        new TupleEntry( inputFields, new Tuple( "1", "10" ) ),
        new TupleEntry( inputFields, new Tuple( "2", "20" ) )
      } );

    assertFalse( results[ 0 ] );
    assertFalse( results[ 1 ] );
    }

  @Test
  public void testXorExpression()
    {
    Fields inputFields = new Fields( "a", "b" );
    ExpressionFilter f1 = new ExpressionFilter( "( 100f < a )", new String[]{"a"}, new Class<?>[]{Float.TYPE} );
    ExpressionFilter f2 = new ExpressionFilter( "( 100f < b )", new String[]{"b"}, new Class<?>[]{Float.TYPE} );
    Xor logic = new Xor( new Fields( "a" ), f1, new Fields( "b" ), f2 );

    boolean[] results = invokeFilter( logic,
      new TupleEntry[]{
        new TupleEntry( inputFields, new Tuple( "1", "10" ) ),
        new TupleEntry( inputFields, new Tuple( "2", "20" ) )
      } );

    assertFalse( results[ 0 ] );
    assertFalse( results[ 1 ] );
    }

  @Test
  public void testAndExpression()
    {
    Fields inputFields = new Fields( "a", "b" );
    ExpressionFilter f1 = new ExpressionFilter( "( 100f < a )", new String[]{"a"}, new Class<?>[]{Float.TYPE} );
    ExpressionFilter f2 = new ExpressionFilter( "( 100f < b )", new String[]{"b"}, new Class<?>[]{Float.TYPE} );
    And logic = new And( new Fields( "a" ), f1, new Fields( "b" ), f2 );

    boolean[] results = invokeFilter( logic,
      new TupleEntry[]{
        new TupleEntry( inputFields, new Tuple( "1", "10" ) ),
        new TupleEntry( inputFields, new Tuple( "2", "20" ) )
      } );

    assertFalse( results[ 0 ] );
    assertFalse( results[ 1 ] );
    }
  }