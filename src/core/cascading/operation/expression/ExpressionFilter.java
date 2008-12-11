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

package cascading.operation.expression;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import org.codehaus.janino.ExpressionEvaluator;

/**
 * Class ExpressionFilter dynamically resolves a given expression using argument {@link Tuple} values. Any Tuple that
 * returns true for the given expression will be removed from the stream. This {@link Filter}
 * is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * Specifially this filter uses the {@link ExpressionEvaluator}, thus the syntax from that class is inherited here.
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
public class ExpressionFilter extends ExpressionOperation implements Filter<ExpressionOperation.Context>
  {
  /**
   * Constructor ExpressionFilter creates a new ExpressionFilter instance.
   *
   * @param expression     of type String
   * @param parameterType of type Class
   */
  public ExpressionFilter( String expression, Class parameterType )
    {
    super( expression, parameterType );
    }

  /**
   * Constructor ExpressionFilter creates a new ExpressionFilter instance.
   *
   * @param expression of type String
   * @param parameterNames of type String[]
   * @param parameterTypes of type Class[]
   */
  public ExpressionFilter( String expression, String[] parameterNames, Class[] parameterTypes )
    {
    super( expression, parameterNames, parameterTypes );
    }

  /** @see cascading.operation.Filter#isRemove(cascading.flow.FlowProcess,cascading.operation.FilterCall) */
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Context> filterCall )
    {
    return (Boolean) evaluate( filterCall.getContext(), filterCall.getArguments() );
    }
  }