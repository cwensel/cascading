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

/**
 * Class ScriptFilter dynamically resolves a given expression using argument {@link cascading.tuple.Tuple} values.
 * This {@link cascading.operation.Filter} is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * Specifically this filter uses the {@link org.codehaus.janino.ScriptEvaluator},
 * thus the syntax from that class is inherited here.
 * <p/>
 * A script may use field names directly as parameters in the expression, or field positions with the syntax
 * "$n", where n is an integer.
 * <p/>
 * Given an argument tuple with the fields "a" and "b", the following script returns true: <br/>
 * <code>boolean result = (a + b == $0 + $1);</code><br/>
 * <code>return boolean;</code><br/>
 * <p/>
 * Unlike an "expression" used by {@link ExpressionFilter}, a "script" requires each line to end in an semi-colon
 * (@{code ;}) and the final line to be a {@code return} statement.
 * <p/>
 * Further, the types of the tuple elements will be coerced into the given parameterTypes. Regardless of the actual
 * tuple element values, they will be converted to the types expected by the script if possible.
 */
public class ScriptFilter extends ScriptOperation implements Filter<ScriptOperation.Context>
  {
  /**
   * Constructor ScriptFilter creates a new ScriptFilter instance.
   *
   * @param script of type String
   */
  @ConstructorProperties({"script"})
  public ScriptFilter( String script )
    {
    super( ANY, script, Boolean.class );
    }

  /**
   * Constructor ScriptFilter creates a new ScriptFilter instance.
   *
   * @param script        of type String
   * @param parameterName of type String
   * @param parameterType of type Class
   */
  @ConstructorProperties({"script", "parameterName", "parameterType"})
  public ScriptFilter( String script, String parameterName, Class parameterType )
    {
    super( 1, script, Boolean.class, new String[]{parameterName}, new Class[]{parameterType} );
    }

  /**
   * Constructor ScriptFilter creates a new ScriptFilter instance.
   * <p/>
   * This constructor will use the runtime {@link cascading.operation.OperationCall#getArgumentFields()}
   * to source the {@code parameterNames} and {@code parameterTypes} required by the other constructors, but
   * use {@code expectedTypes} to coerce the incoming types to before passing as parameters to the expression.
   *
   * @param script        of type String
   * @param expectedTypes of type Class[]
   */
  @ConstructorProperties({"script", "expectedTypes"})
  public ScriptFilter( String script, Class[] expectedTypes )
    {
    super( expectedTypes.length, script, Boolean.class, expectedTypes );
    }

  /**
   * Constructor ScriptFilter creates a new ScriptFilter instance.
   *
   * @param script         of type String
   * @param parameterNames of type String[]
   * @param parameterTypes of type Class[]
   */
  @ConstructorProperties({"script", "parameterNames", "parameterTypes"})
  public ScriptFilter( String script, String[] parameterNames, Class[] parameterTypes )
    {
    super( parameterTypes.length, script, Boolean.class, parameterNames, parameterTypes );
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Context> filterCall )
    {
    return (Boolean) evaluate( filterCall.getContext(), filterCall.getArguments() );
    }
  }