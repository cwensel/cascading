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

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;
import org.codehaus.janino.ExpressionEvaluator;

/**
 * Class ExpressionFilter dynamically resolves a given expression using argument {@link Tuple} values. Any Tuple that
 * returns true for the given expression will be removed from the stream. This {@link Filter}
 * is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * Specifically this filter uses the {@link ExpressionEvaluator}, thus the syntax from that class is inherited here.
 * <p/>
 * An expression may use field names directly as parameters in the expression, or field positions with the syntax
 * "$n", where n is an integer.
 * <p/>
 * Given an argument tuple with the fields "a" and "b", the following expression returns true: <br/>
 * <code>a + b == $0 + $1</code><br/>
 * <p/>
 * Further, the types of the tuple elements will be coerced into the given parameterTypes. Regardless of the actual
 * tuple element values, they will be converted to the types expected by the expression.
 * <p/>
 * Field names used in the expression should be valid Java variable names; for example, '+' or '-' are not allowed.
 * Also the use of a field name that begins with an upper-case character is likely to fail and should be avoided.
 */
public class ExpressionFilter extends ExpressionOperation implements Filter<ScriptOperation.Context>
  {
  /**
   * Constructor ExpressionFilter creates a new ExpressionFilter instance.
   *
   * @param expression    of type String
   * @param parameterType of type Class
   */
  @ConstructorProperties({"expression", "parameterType"})
  public ExpressionFilter( String expression, Class parameterType )
    {
    super( expression, parameterType );
    }

  /**
   * Constructor ExpressionFilter creates a new ExpressionFilter instance.
   *
   * @param expression     of type String
   * @param parameterNames of type String[]
   * @param parameterTypes of type Class[]
   */
  @ConstructorProperties({"expression", "parameterNames", "parameterTypes"})
  public ExpressionFilter( String expression, String[] parameterNames, Class[] parameterTypes )
    {
    super( expression, parameterNames, parameterTypes );
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Context> filterCall )
    {
    return (Boolean) evaluate( filterCall.getContext(), filterCall.getArguments() );
    }
  }