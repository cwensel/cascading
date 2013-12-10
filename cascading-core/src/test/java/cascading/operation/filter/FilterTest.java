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

package cascading.operation.filter;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class FilterTest extends CascadingTestCase
  {
  public FilterTest()
    {
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    }

  public void testNotNull()
    {
    Filter filter = new FilterNotNull();

    assertTrue( invokeFilter( filter, new Tuple( 1 ) ) );
    assertFalse( invokeFilter( filter, new Tuple( (Comparable) null ) ) );
    assertTrue( invokeFilter( filter, new Tuple( "0", 1 ) ) );
    assertTrue( invokeFilter( filter, new Tuple( "0", null ) ) );
    assertFalse( invokeFilter( filter, new Tuple( null, null ) ) );
    }

  public void testNull()
    {
    Filter filter = new FilterNull();

    assertFalse( invokeFilter( filter, new Tuple( 1 ) ) );
    assertTrue( invokeFilter( filter, new Tuple( (Comparable) null ) ) );

    assertFalse( invokeFilter( filter, new Tuple( "0", 1 ) ) );
    assertTrue( invokeFilter( filter, new Tuple( "0", null ) ) );
    assertTrue( invokeFilter( filter, new Tuple( null, null ) ) );
    }

  public class BooleanFilter extends BaseOperation implements Filter
    {
    private final boolean result;

    public BooleanFilter( boolean result )
      {
      this.result = result;
      }

    public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
      {
      return result;
      }
    }

  public void testAnd()
    {
    Fields[] fields = new Fields[]{new Fields( 0 ), new Fields( 1 )};

    Filter[] filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( true )};
    Filter filter = new And( fields, filters );

    assertTrue( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( false )};
    filter = new And( fields, filters );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( true )};
    filter = new And( fields, filters );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( false )};
    filter = new And( fields, filters );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );
    }

  public void testOr()
    {
    Fields[] fields = new Fields[]{new Fields( 0 ), new Fields( 1 )};

    Filter[] filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( true )};
    Filter filter = new Or( fields, filters );

    assertTrue( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( false )};
    filter = new Or( fields, filters );

    assertTrue( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( true )};
    filter = new Or( fields, filters );

    assertTrue( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( false )};
    filter = new Or( fields, filters );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );
    }

  public void testXor()
    {
    Fields[] fields = new Fields[]{new Fields( 0 ), new Fields( 1 )};

    Filter[] filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( true )};
    Filter filter = new Xor( fields[ 0 ], filters[ 0 ], fields[ 1 ], filters[ 1 ] );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( false )};
    filter = new Xor( fields[ 0 ], filters[ 0 ], fields[ 1 ], filters[ 1 ] );

    assertTrue( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( true )};
    filter = new Xor( fields[ 0 ], filters[ 0 ], fields[ 1 ], filters[ 1 ] );

    assertTrue( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( false )};
    filter = new Xor( fields[ 0 ], filters[ 0 ], fields[ 1 ], filters[ 1 ] );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );
    }

  public void testNot()
    {
    Fields[] fields = new Fields[]{new Fields( 0 ), new Fields( 1 )};

    Filter[] filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( true )};
    Filter filter = new Not( new Or( fields, filters ) );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( true ), new BooleanFilter( false )};
    filter = new Not( new Or( fields, filters ) );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( true )};
    filter = new Not( new Or( fields, filters ) );

    assertFalse( invokeFilter( filter, new Tuple( 1, 2 ) ) );

    filters = new Filter[]{new BooleanFilter( false ), new BooleanFilter( false )};
    filter = new Not( new Or( fields, filters ) );

    assertTrue( invokeFilter( filter, new Tuple( 1, 2 ) ) );
    }

  public void testPartialDuplicates()
    {
    Filter filter = new Unique.FilterPartialDuplicates( 2 );

    Tuple[] tuples = new Tuple[]{new Tuple( 1 ), // false - first time
                                 new Tuple( 1 ), // true - second time
                                 new Tuple( (Comparable) null ), // false
                                 new Tuple( (Comparable) null ), // true - make lots, its a LRU
                                 new Tuple( (Comparable) null ), // true
                                 new Tuple( (Comparable) null ), // true
                                 new Tuple( 1 ), // true - holds two, so still cached
                                 new Tuple( 2 ), // false - force least recently seen out
                                 new Tuple( 1 )}; // false

    boolean[] expected = new boolean[]{false, true, false, true, true, true, true, false, false};

    boolean[] results = invokeFilter( filter, tuples );

    for( int i = 0; i < results.length; i++ )
      assertEquals( "failed on: " + i, expected[ i ], results[ i ] );
    }

  // these tests verify an Expression can be safely nested.

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