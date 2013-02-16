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
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.codehaus.janino.ExpressionEvaluator;

/**
 * Class ExpressionFunction dynamically resolves a given expression using argument {@link Tuple} values. This {@link Function}
 * is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * Specifically this function uses the {@link ExpressionEvaluator}, thus the syntax from that class is inherited here.
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
public class ExpressionFunction extends ExpressionOperation implements Function<ScriptOperation.Context>
  {
  /**
   * Constructor ExpressionFunction creates a new ExpressionFunction instance.
   * <p/>
   * This constructor assumes the given expression expects no input parameters. This is useful when
   * inserting random numbers for example, {@code (int) (Math.random() * Integer.MAX_VALUE) }.
   *
   * @param fieldDeclaration of type Fields
   * @param expression       of type String
   */
  @ConstructorProperties({"fieldDeclaration", "expression"})
  public ExpressionFunction( Fields fieldDeclaration, String expression )
    {
    super( fieldDeclaration, expression );

    verify( fieldDeclaration );
    }

  /**
   * Constructor ExpressionFunction creates a new ExpressionFunction instance.
   * <p/>
   * This constructor assumes all parameter are of the same type.
   *
   * @param fieldDeclaration of type Fields
   * @param expression       of type String
   * @param parameterType    of type Class
   */
  @ConstructorProperties({"fieldDeclaration", "expression", "parameterType"})
  public ExpressionFunction( Fields fieldDeclaration, String expression, Class parameterType )
    {
    super( fieldDeclaration, expression, parameterType );

    verify( fieldDeclaration );
    }

  /**
   * Constructor ExpressionFunction creates a new ExpressionFunction instance.
   * <p/>
   * This constructor expects all parameter type names to be declared with their types. Positional parameters must
   * be named the same as in the given expression with the "$" sign prepended.
   *
   * @param fieldDeclaration of type Fields
   * @param expression       of type String
   * @param parameterNames   of type String[]
   * @param parameterTypes   of type Class[]
   */
  @ConstructorProperties({"fieldDeclaration", "expression", "parameterNames", "parameterTypes"})
  public ExpressionFunction( Fields fieldDeclaration, String expression, String[] parameterNames, Class[] parameterTypes )
    {
    super( fieldDeclaration, expression, parameterNames, parameterTypes );

    verify( fieldDeclaration );
    }

  private void verify( Fields fieldDeclaration )
    {
    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field, was " + fieldDeclaration.print() );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<ExpressionOperation.Context> functionCall )
    {
    functionCall.getContext().result.set( 0, evaluate( functionCall.getContext(), functionCall.getArguments() ) );
    functionCall.getOutputCollector().add( functionCall.getContext().result );
    }
  }
