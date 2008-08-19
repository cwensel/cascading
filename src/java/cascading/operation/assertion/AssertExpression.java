/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.assertion;

import cascading.operation.ValueAssertion;
import cascading.operation.expression.ExpressionOperation;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class AssertExpression dynamically resolves a given expression using argument {@link cascading.tuple.Tuple} values. Any Tuple that
 * returns true for the given expression pass the assertion. This {@link cascading.operation.Assertion}
 * is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * Specifially this filter uses the {@link org.codehaus.janino.ExpressionEvaluator}, thus the syntax from that class is inherited here.
 * <p/>
 * An expression may use field names directly as parameters in the expression, or field positions with the syntax
 * "$n", where n is an integer.
 * <p/>
 * Given an argument tuple with the fields "a" and "b", the following expression returns true: <br/>
 * <code>a + b == $0 + $1</code><br/>
 * <p/>
 * Further, the types of the tuple elements will be coerced into the given parameterTypes. Regardless of the actual
 * tuple element values, they will be converted to the types expected by the expression.
 */
public class AssertExpression extends ExpressionOperation implements ValueAssertion
  {
  /**
   * Constructor ExpressionFilter creates a new ExpressionFilter instance.
   *
   * @param expression     of type String
   * @param parameterTypes of type Class[]
   */
  public AssertExpression( String expression, Class... parameterTypes )
    {
    super( Fields.ALL, expression, parameterTypes );
    }

  /** @see cascading.operation.ValueAssertion#doAssert(TupleEntry) */
  public void doAssert( TupleEntry input )
    {
    if( !(Boolean) evaluate( input ) )
      BaseAssertion.throwFail( "argument tuple: %s did not evaluate to true with expression: %s", input.getTuple().print(), expression );
    }
  }