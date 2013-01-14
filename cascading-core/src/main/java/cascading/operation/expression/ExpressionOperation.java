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
import java.io.IOException;
import java.io.StringReader;

import cascading.operation.OperationException;
import cascading.tuple.Fields;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.ScriptEvaluator;

import static cascading.tuple.coerce.Coercions.asClass;

/**
 * Class ExpressionOperation is the base class for {@link ExpressionFunction}, {@link ExpressionFilter},
 * {@link cascading.operation.assertion.AssertExpression}.
 */
public class ExpressionOperation extends ScriptOperation
  {
  @ConstructorProperties({"fieldDeclaration", "expression"})
  protected ExpressionOperation( Fields fieldDeclaration, String expression )
    {
    this( fieldDeclaration, expression, new String[ 0 ], new Class[ 0 ] );
    }

  @ConstructorProperties({"fieldDeclaration", "expression", "parameterType"})
  protected ExpressionOperation( Fields fieldDeclaration, String expression, Class parameterType )
    {
    super( 1, fieldDeclaration, expression, asClass( fieldDeclaration.getType( 0 ) ), null,
      new Class[]{parameterType} );
    }

  @ConstructorProperties({"fieldDeclaration", "expression", "parameterNames", "parameterTypes"})
  protected ExpressionOperation( Fields fieldDeclaration, String expression, String[] parameterNames, Class[] parameterTypes )
    {
    super( parameterTypes.length, fieldDeclaration, expression, asClass( fieldDeclaration.getType( 0 ) ), parameterNames, parameterTypes );
    }

  @ConstructorProperties({"expression", "parameterType"})
  protected ExpressionOperation( String expression, Class parameterType )
    {
    super( 1, expression, Object.class, null, new Class[]{parameterType} );
    }

  @ConstructorProperties({"expression", "parameterNames", "parameterTypes"})
  protected ExpressionOperation( String expression, String[] parameterNames, Class[] parameterTypes )
    {
    super( parameterTypes.length, expression, Object.class, parameterNames, parameterTypes );
    }

  protected String[] guessParameterNames() throws CompileException, IOException
    {
    return ExpressionEvaluator.guessParameterNames( new Scanner( "expressionEval", new StringReader( block ) ) );
    }

  @Override
  protected ScriptEvaluator getEvaluator( Class returnType, String[] parameterNames, Class[] parameterTypes )
    {
    try
      {
      return new ExpressionEvaluator( block, getReturnType(), parameterNames, parameterTypes );
      }
    catch( CompileException exception )
      {
      throw new OperationException( "could not compile expression: " + block, exception );
      }
    }
  }
