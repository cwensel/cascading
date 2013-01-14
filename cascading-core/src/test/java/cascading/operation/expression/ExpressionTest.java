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

package cascading.operation.expression;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class ExpressionTest extends CascadingTestCase
  {
  public ExpressionTest()
    {
    }

  public void testSimpleExpression()
    {
    assertEquals( 3, evaluate( "a + b", int.class, getEntry( 1, 2 ) ) );
    assertEquals( 3, evaluate( "a + b", int.class, getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3, evaluate( "a + b", int.class, getEntry( "1", 2.0 ) ) );

    String[] names = new String[]{"a", "b"};
    Class[] types = new Class[]{long.class, int.class};

    assertEquals( 3l, evaluate( "a + b", names, types, getEntry( 1, 2 ) ) );
    assertEquals( 3l, evaluate( "a + b", names, types, getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3l, evaluate( "a + b", names, types, getEntry( "1", 2.0 ) ) );

    types = new Class[]{double.class, int.class};

    assertEquals( 3d, evaluate( "a + b", names, types, getEntry( 1, 2 ) ) );
    assertEquals( 3d, evaluate( "a + b", names, types, getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3d, evaluate( "a + b", names, types, getEntry( "1", 2.0 ) ) );

    assertEquals( 3, evaluate( "$0 + $1", int.class, getEntry( 1, 2 ) ) );
    assertEquals( 3, evaluate( "$0 + $1", int.class, getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3, evaluate( "$0 + $1", int.class, getEntry( "1", 2.0 ) ) );

    names = new String[]{"a", "b"};
    types = new Class[]{String.class, int.class};
    assertEquals( true, evaluate( "(a != null) && (b > 0)", names, types, getEntry( "1", 2.0 ) ) );

    names = new String[]{"$0", "$1"};
    types = new Class[]{String.class, int.class};
    assertEquals( true, evaluate( "($0 != null) && ($1 > 0)", names, types, getEntry( "1", 2.0 ) ) );

    names = new String[]{"a", "b", "c"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( true, evaluate( "b.equals(\"1\") && (a == 2.0) && c.equals(\"2\")", names, types, getEntry( 2.0, "1", "2" ) ) );

    names = new String[]{"a", "b", "$2"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( true, evaluate( "b.equals(\"1\") && (a == 2.0) && $2.equals(\"2\")", names, types, getEntry( 2.0, "1", "2" ) ) );
    }

  public void testNoParamExpression()
    {
    String expression = "(int) (Math.random() * Integer.MAX_VALUE)";
    Number integer = (Number) evaluate( expression, getEntry( "1", 2.0 ) );
//    System.out.println( "integer = " + integer );
    assertNotNull( integer );

    try
      {
      evaluate( "(int) (Math.random() * Integer.MAX_VALUE) + parameter", getEntry( "1", 2.0 ) );
      fail( "should throw exception" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  private Object evaluate( String expression, TupleEntry tupleEntry )
    {
    ExpressionFunction function = getFunction( expression );

    ConcreteCall<ExpressionOperation.Context> call = new ConcreteCall<ExpressionOperation.Context>( tupleEntry.getFields(), function.getFieldDeclaration() );
    function.prepare( FlowProcess.NULL, call );

    return function.evaluate( call.getContext(), tupleEntry );
    }

  private Object evaluate( String expression, Class type, TupleEntry tupleEntry )
    {
    ExpressionFunction function = getFunction( expression, type );

    ConcreteCall<ExpressionOperation.Context> call = new ConcreteCall<ExpressionOperation.Context>( tupleEntry.getFields(), function.getFieldDeclaration() );
    function.prepare( FlowProcess.NULL, call );

    return function.evaluate( call.getContext(), tupleEntry );
    }

  private Object evaluate( String expression, String[] names, Class[] types, TupleEntry tupleEntry )
    {
    ExpressionFunction function = getFunction( expression, names, types );

    ConcreteCall<ExpressionOperation.Context> call = new ConcreteCall<ExpressionOperation.Context>( tupleEntry.getFields(), function.getFieldDeclaration() );
    function.prepare( FlowProcess.NULL, call );

    return function.evaluate( call.getContext(), tupleEntry );
    }

  private ExpressionFunction getFunction( String expression )
    {
    return new ExpressionFunction( new Fields( "result" ), expression );
    }

  private ExpressionFunction getFunction( String expression, Class type )
    {
    return new ExpressionFunction( new Fields( "result" ), expression, type );
    }

  private ExpressionFunction getFunction( String expression, String[] names, Class[] classes )
    {
    return new ExpressionFunction( new Fields( "result" ), expression, names, classes );
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
