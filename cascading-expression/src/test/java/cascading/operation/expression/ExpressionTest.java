/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import org.junit.Test;

/**
 *
 */
public class ExpressionTest extends CascadingTestCase
  {
  public ExpressionTest()
    {
    }

  @Test
  public void testExpressionFunction()
    {
    assertEquals( 3, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", int.class ), getEntry( 1, 2 ) ) );
    assertEquals( 3, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", int.class ), getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", int.class ), getEntry( "1", 2.0 ) ) );

    String[] names = new String[]{"a", "b"};
    Class[] types = new Class[]{long.class, int.class};

    assertEquals( 3l, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", names, types ), getEntry( 1, 2 ) ) );
    assertEquals( 3l, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", names, types ), getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3l, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", names, types ), getEntry( "1", 2.0 ) ) );

    types = new Class[]{double.class, int.class};

    assertEquals( 3d, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", names, types ), getEntry( 1, 2 ) ) );
    assertEquals( 3d, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", names, types ), getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3d, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b", names, types ), getEntry( "1", 2.0 ) ) );

    Fields arguments = new Fields( names ).applyTypes( types );

    assertEquals( 3d, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b" ), getEntry( arguments, 1, 2 ) ) );
    assertEquals( 3d, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b" ), getEntry( arguments, 1.0, 2.0 ) ) );
    assertEquals( 3d, evaluate( new ExpressionFunction( new Fields( "result" ), "a + b" ), getEntry( arguments, "1", 2.0 ) ) );

    assertEquals( 3, evaluate( new ExpressionFunction( new Fields( "result" ), "$0 + $1", int.class ), getEntry( 1, 2 ) ) );
    assertEquals( 3, evaluate( new ExpressionFunction( new Fields( "result" ), "$0 + $1", int.class ), getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3, evaluate( new ExpressionFunction( new Fields( "result" ), "$0 + $1", int.class ), getEntry( "1", 2.0 ) ) );

    names = new String[]{"a", "b"};
    types = new Class[]{String.class, int.class};
    assertEquals( true, evaluate( new ExpressionFunction( new Fields( "result" ), "(a != null) && (b > 0)", names, types ), getEntry( "1", 2.0 ) ) );

    names = new String[]{"$0", "$1"};
    types = new Class[]{String.class, int.class};
    assertEquals( true, evaluate( new ExpressionFunction( new Fields( "result" ), "($0 != null) && ($1 > 0)", names, types ), getEntry( "1", 2.0 ) ) );

    names = new String[]{"a", "b", "c"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( true, evaluate( new ExpressionFunction( new Fields( "result" ), "b.equals(\"1\") && (a == 2.0) && c.equals(\"2\")", names, types ), getEntry( 2.0, "1", "2" ) ) );

    names = new String[]{"a", "b", "$2"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( true, evaluate( new ExpressionFunction( new Fields( "result" ), "b.equals(\"1\") && (a == 2.0) && $2.equals(\"2\")", names, types ), getEntry( 2.0, "1", "2" ) ) );
    }

  @Test
  public void testExpressionFilter()
    {
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", int.class ), getEntry( 1, 2 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", int.class ), getEntry( 1.0, 2.0 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", int.class ), getEntry( "1", 2.0 ) ) );

    String[] names = new String[]{"a", "b"};
    Class[] types = new Class[]{long.class, int.class};

    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", names, types ), getEntry( 1, 2 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", names, types ), getEntry( 1.0, 2.0 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", names, types ), getEntry( "1", 2.0 ) ) );

    types = new Class[]{double.class, int.class};

    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", names, types ), getEntry( 1, 2 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", names, types ), getEntry( 1.0, 2.0 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b", names, types ), getEntry( "1", 2.0 ) ) );

    Fields arguments = new Fields( names ).applyTypes( types );

    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b" ), getEntry( arguments, 1, 2 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b" ), getEntry( arguments, 1.0, 2.0 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "a < b" ), getEntry( arguments, "1", 2.0 ) ) );

    assertEquals( true, invokeFilter( new ExpressionFilter( "$0 < $1", int.class ), getEntry( 1, 2 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "$0 < $1", int.class ), getEntry( 1.0, 2.0 ) ) );
    assertEquals( true, invokeFilter( new ExpressionFilter( "$0 < $1", int.class ), getEntry( "1", 2.0 ) ) );

    names = new String[]{"a", "b"};
    types = new Class[]{String.class, int.class};
    assertEquals( true, invokeFilter( new ExpressionFilter( "(a != null) && (b > 0)", names, types ), getEntry( "1", 2.0 ) ) );

    names = new String[]{"$0", "$1"};
    types = new Class[]{String.class, int.class};
    assertEquals( true, invokeFilter( new ExpressionFilter( "($0 != null) && ($1 > 0)", names, types ), getEntry( "1", 2.0 ) ) );

    names = new String[]{"a", "b", "c"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( true, invokeFilter( new ExpressionFilter( "b.equals(\"1\") && (a == 2.0) && c.equals(\"2\")", names, types ), getEntry( 2.0, "1", "2" ) ) );

    names = new String[]{"a", "b", "$2"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( true, invokeFilter( new ExpressionFilter( "b.equals(\"1\") && (a == 2.0) && $2.equals(\"2\")", names, types ), getEntry( 2.0, "1", "2" ) ) );
    }

  @Test
  public void testNoParamExpression()
    {
    Fields fields = new Fields( "a", "b" ).applyTypes( String.class, double.class );
    String expression = "(int) (Math.random() * Integer.MAX_VALUE)";
    Number integer = (Number) evaluate( new ExpressionFunction( new Fields( "result" ), expression ), getEntry( fields, "1", 2.0 ) );
    assertNotNull( integer );

    // Fields.NONE as argument selector
    integer = (Number) evaluate( new ExpressionFunction( new Fields( "result" ), expression ), TupleEntry.NULL );
    assertNotNull( integer );

    try
      {
      evaluate( new ExpressionFunction( new Fields( "result" ), "(int) (Math.random() * Integer.MAX_VALUE) + parameter" ), getEntry( fields, "1", 2.0 ) );
      fail( "should throw exception" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  private Object evaluate( ExpressionFunction function, TupleEntry tupleEntry )
    {
    TupleListCollector tuples = invokeFunction( function, tupleEntry, function.getFieldDeclaration() );

    return tuples.entryIterator().next().getObject( 0 );
    }

  private TupleEntry getEntry( Fields fields, Comparable lhs, Comparable rhs )
    {
    Tuple parameters = new Tuple( lhs, rhs );

    return new TupleEntry( fields, parameters );
    }

  private TupleEntry getEntry( Comparable lhs, Comparable rhs )
    {
    Fields fields = new Fields( "a", "b" );
    Tuple parameters = new Tuple( lhs, rhs );

    return new TupleEntry( fields, parameters );
    }

  private TupleEntry getEntry( Comparable f, Comparable s, Comparable t )
    {
    Fields fields = new Fields( "a", "b", "c" );
    Tuple parameters = new Tuple( f, s, t );

    return new TupleEntry( fields, parameters );
    }
  }
