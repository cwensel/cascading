/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.operation.expression;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import junit.framework.TestCase;

/**
 *
 */
public class ExpressionTest extends TestCase
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
