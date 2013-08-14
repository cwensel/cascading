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

import static cascading.tuple.coerce.Coercions.asClass;

/**
 * Class ScriptFunction dynamically resolves a given expression using argument {@link cascading.tuple.Tuple} values.
 * This {@link cascading.operation.Function} is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * This class is different from {@link ScriptTupleFunction} in that it allows any return type instance to be returned
 * by the script. ScriptTupleFunction allows only a single {@code Tuple} instance to be returned.
 * <p/>
 * Specifically this function uses the {@link org.codehaus.janino.ScriptEvaluator},
 * thus the syntax from that class is inherited here.
 * <p/>
 * A script may use field names directly as parameters in the expression, or field positions with the syntax
 * "$n", where n is an integer.
 * <p/>
 * Given an argument tuple with the fields "a" and "b", the following script returns true: <br/>
 * <code>boolean result = (a + b == $0 + $1);</code><br/>
 * <code>return boolean;</code><br/>
 * <p/>
 * Unlike an "expression" used by {@link ExpressionFunction}, a "script" requires each line to end in an semi-colon
 * (@{code ;}) and the final line to be a {@code return} statement.
 * <p/>
 * Further, the types of the tuple elements will be coerced into the given parameterTypes. Regardless of the actual
 * tuple element values, they will be converted to the types expected by the script if possible.
 */
public class ScriptFunction extends ScriptOperation implements Function<ScriptOperation.Context>
  {
  /**
   * Constructor ScriptFunction creates a new ScriptFunction instance.
   * <p/>
   * This constructor will use the runtime {@link cascading.operation.OperationCall#getArgumentFields()}
   * to source the {@code parameterNames} and {@code parameterTypes} required by the other constructors.
   * <p/>
   * The {@code returnType} will be retrieved from the given {@code fieldDeclaration.getTypeClass(0)}.
   *
   * @param fieldDeclaration of type Fields
   * @param script           of type String
   */
  @ConstructorProperties({"fieldDeclaration", "script"})
  public ScriptFunction( Fields fieldDeclaration, String script )
    {
    super( ANY, fieldDeclaration, script );

    verify( fieldDeclaration );
    }

  /**
   * Constructor ScriptFunction creates a new ScriptFunction instance.
   * <p/>
   * This constructor will use the runtime {@link cascading.operation.OperationCall#getArgumentFields()}
   * to source the {@code parameterNames} and {@code parameterTypes} required by the other constructors.
   *
   * @param fieldDeclaration of type Fields
   * @param script           of type String
   * @param returnType       of type Class
   */
  @ConstructorProperties({"fieldDeclaration", "script", "returnType"})
  public ScriptFunction( Fields fieldDeclaration, String script, Class returnType )
    {
    super( ANY, fieldDeclaration, script, returnType );

    verify( fieldDeclaration );
    }

  /**
   * Constructor ScriptFunction creates a new ScriptFunction instance.
   * <p/>
   * This constructor will use the runtime {@link cascading.operation.OperationCall#getArgumentFields()}
   * to source the {@code parameterNames} and {@code parameterTypes} required by the other constructors, but
   * use {@code expectedTypes} to coerce the incoming types to before passing as parameters to the expression.
   *
   * @param fieldDeclaration of type Fields
   * @param script           of type String
   * @param returnType       of type Class
   * @param expectedTypes    of type Class[]
   */
  @ConstructorProperties({"fieldDeclaration", "script", "returnType", "expectedTypes"})
  public ScriptFunction( Fields fieldDeclaration, String script, Class returnType, Class[] expectedTypes )
    {
    super( expectedTypes.length, fieldDeclaration, script, returnType, expectedTypes );

    verify( fieldDeclaration );
    }

  /**
   * Constructor ScriptFunction creates a new ScriptFunction instance.
   * <p/>
   * This constructor expects all parameter type names to be declared with their types. Positional parameters must
   * be named the same as in the given script with the "$" sign prepended.
   *
   * @param fieldDeclaration of type Fields
   * @param script           of type String
   * @param returnType       of type Class
   * @param parameterNames   of type String[]
   * @param parameterTypes   of type Class[]
   */
  @ConstructorProperties({"fieldDeclaration", "script", "returnType", "parameterNames", "parameterTypes"})
  public ScriptFunction( Fields fieldDeclaration, String script, Class returnType, String[] parameterNames, Class[] parameterTypes )
    {
    super( parameterTypes.length, fieldDeclaration, script, returnType, parameterNames, parameterTypes );

    verify( fieldDeclaration );
    }

  /**
   * Constructor ScriptFunction creates a new ScriptFunction instance.
   * <p/>
   * This constructor expects all parameter type names to be declared with their types. Positional parameters must
   * be named the same as in the given script with the "$" sign prepended.
   *
   * @param fieldDeclaration of type Fields
   * @param script           of type String
   * @param parameterNames   of type String[]
   * @param parameterTypes   of type Class[]
   */
  @ConstructorProperties({"fieldDeclaration", "script", "parameterNames", "parameterTypes"})
  public ScriptFunction( Fields fieldDeclaration, String script, String[] parameterNames, Class[] parameterTypes )
    {
    super( parameterTypes.length, fieldDeclaration, script, asClass( fieldDeclaration.getType( 0 ) ), parameterNames, parameterTypes );

    verify( fieldDeclaration );
    }

  private void verify( Fields fieldDeclaration )
    {
    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field, was " + fieldDeclaration.print() );
    }

  public String getScript()
    {
    return getBlock();
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Context> functionCall )
    {
    functionCall.getContext().result.set( 0, evaluate( functionCall.getContext(), functionCall.getArguments() ) );
    functionCall.getOutputCollector().add( functionCall.getContext().result );
    }
  }
