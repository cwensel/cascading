/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.operation.expression;

import cascading.CascadingTestCase;
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
    super( "expression test" );
    }

  public void testSimpleExpression()
    {
    assertEquals( 3, getFunction( "a + b", int.class, int.class ).evaluate( getEntry( 1, 2 ) ) );
    assertEquals( 3, getFunction( "a + b", int.class, int.class ).evaluate( getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3, getFunction( "a + b", int.class, int.class ).evaluate( getEntry( "1", 2.0 ) ) );

    assertEquals( 3l, getFunction( "a + b", long.class, int.class ).evaluate( getEntry( 1, 2 ) ) );
    assertEquals( 3l, getFunction( "a + b", long.class, int.class ).evaluate( getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3l, getFunction( "a + b", long.class, int.class ).evaluate( getEntry( "1", 2.0 ) ) );

    assertEquals( 3d, getFunction( "a + b", double.class, int.class ).evaluate( getEntry( 1, 2 ) ) );
    assertEquals( 3d, getFunction( "a + b", double.class, int.class ).evaluate( getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3d, getFunction( "a + b", double.class, int.class ).evaluate( getEntry( "1", 2.0 ) ) );

    assertEquals( 3, getFunction( "$0 + $1", int.class, int.class ).evaluate( getEntry( 1, 2 ) ) );
    assertEquals( 3, getFunction( "$0 + $1", int.class, int.class ).evaluate( getEntry( 1.0, 2.0 ) ) );
    assertEquals( 3, getFunction( "$0 + $1", int.class, int.class ).evaluate( getEntry( "1", 2.0 ) ) );
    }

  private ExpressionFunction getFunction( String expression, Class... classes )
    {
    ExpressionFunction function = new ExpressionFunction( new Fields( "result" ), expression, classes );
    return function;
    }

  private TupleEntry getEntry( Comparable lhs, Comparable rhs )
    {
    Fields fields = new Fields( "a", "b" );
    Tuple parameters = new Tuple( lhs, rhs );

    return new TupleEntry( fields, parameters );
    }
  }
