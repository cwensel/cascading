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

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;
import org.codehaus.janino.ExpressionEvaluator;

/**
 * Class ExpressionFunction dynamically resolves a given expression using argument {@link Tuple} values. This {@link Function}
 * is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * Specifially this function uses the {@link ExpressionEvaluator}, thus the syntax from that class is inherited here.
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
public class ExpressionFunction extends ExpressionOperation implements Function
  {
  /**
   * Constructor ExpressionFunction creates a new ExpressionFunction instance.
   *
   * @param fieldDeclaration of type Fields
   * @param expression       of type String
   * @param parameterTypes   of type Class[]
   */
  public ExpressionFunction( Fields fieldDeclaration, String expression, Class... parameterTypes )
    {
    super( parameterTypes.length, fieldDeclaration, expression, parameterTypes );

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field, was " + fieldDeclaration.print() );
    }

  /** @see Function#operate(cascading.tuple.TupleEntry,cascading.tuple.TupleCollector) */
  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    outputCollector.add( new Tuple( evaluate( input ) ) );
    }

  }
