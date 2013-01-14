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

package cascading.operation.assertion;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.AssertionLevel;
import cascading.operation.PlannerLevel;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.operation.expression.ExpressionOperation;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class AssertExpression dynamically resolves a given expression using argument {@link cascading.tuple.Tuple} values. Any Tuple that
 * returns true for the given expression pass the assertion. This {@link cascading.operation.Assertion}
 * is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * Specifically this filter uses the {@link org.codehaus.janino.ExpressionEvaluator}, thus the syntax from that class is inherited here.
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
public class AssertExpression extends ExpressionOperation implements ValueAssertion<ExpressionOperation.Context>
  {
  /**
   * Constructor ExpressionFilter creates a new ExpressionFilter instance.
   *
   * @param expression    of type String
   * @param parameterType of type Class
   */
  @ConstructorProperties({"expression", "parameterType"})
  public AssertExpression( String expression, Class parameterType )
    {
    super( Fields.ALL, expression, parameterType );
    }

  /**
   * Constructor AssertExpression creates a new AssertExpression instance.
   *
   * @param fieldDeclaration of type Fields
   * @param expression       of type String
   * @param parameterNames   of type String[]
   * @param parameterTypes   of type Class[]
   */
  @ConstructorProperties({"fieldDeclaration", "expression", "parameterNames", "parameterTypes"})
  public AssertExpression( Fields fieldDeclaration, String expression, String[] parameterNames, Class[] parameterTypes )
    {
    super( fieldDeclaration, expression, parameterNames, parameterTypes );
    }

  @Override
  public boolean supportsPlannerLevel( PlannerLevel plannerLevel )
    {
    return plannerLevel instanceof AssertionLevel;
    }

  @Override
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall<Context> assertionCall )
    {
    TupleEntry input = assertionCall.getArguments();

    if( !(Boolean) evaluate( assertionCall.getContext(), input ) )
      BaseAssertion.throwFail( "argument tuple: %s did not evaluate to true with expression: %s", input.getTuple().print(), block );
    }
  }