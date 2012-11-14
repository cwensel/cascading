/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;

/**
 *
 */
public abstract class ScriptOperation extends BaseOperation<ScriptOperation.Context>
  {
  /** Field expression */
  protected final String block;
  /** Field parameterTypes */
  protected Class[] parameterTypes;
  /** Field parameterNames */
  protected String[] parameterNames;
  /** returnType */
  protected Class returnType = Object.class;

  public ScriptOperation( int numArgs, Fields fieldDeclaration, String block, Class returnType, String[] parameterNames, Class[] parameterTypes )
    {
    super( numArgs, fieldDeclaration );
    this.parameterNames = parameterNames == null ? null : Arrays.copyOf( parameterNames, parameterNames.length );
    this.block = block;
    this.returnType = returnType == null ? this.returnType : returnType;
    this.parameterTypes = Arrays.copyOf( parameterTypes, parameterTypes.length );

    if( getParameterNames().length != getParameterTypes().length )
      throw new IllegalArgumentException( "parameterNames must be same length as parameterTypes" );
    }

  public ScriptOperation( int numArgs, String block, Class returnType, String[] parameterNames, Class[] parameterTypes )
    {
    super( numArgs );
    this.parameterNames = parameterNames == null ? null : Arrays.copyOf( parameterNames, parameterNames.length );
    this.block = block;
    this.returnType = returnType == null ? this.returnType : returnType;
    this.parameterTypes = Arrays.copyOf( parameterTypes, parameterTypes.length );

    if( getParameterNames().length != getParameterTypes().length )
      throw new IllegalArgumentException( "parameterNames must be same length as parameterTypes" );
    }

  private String[] getParameterNames()
    {
    if( parameterNames != null )
      return parameterNames;

    try
      {
      parameterNames = guessParameterNames();
      }
    catch( IOException exception )
      {
      throw new OperationException( "could not read expression: " + block, exception );
      }
    catch( CompileException exception )
      {
      throw new OperationException( "could not compile expression: " + block, exception );
      }

    return parameterNames;
    }

  protected String[] guessParameterNames() throws CompileException, IOException
    {
    throw new OperationException( "parameter names are required" );
    }

  private Fields getParameterFields()
    {
    return makeFields( getParameterNames() );
    }

  private Class[] getParameterTypes()
    {
    if( parameterNames.length == parameterTypes.length )
      return parameterTypes;

    if( parameterTypes.length != 1 )
      throw new IllegalStateException( "wrong number of parameter types, expects: " + parameterNames.length );

    Class[] types = new Class[ parameterNames.length ];

    Arrays.fill( types, parameterTypes[ 0 ] );

    parameterTypes = types;

    return parameterTypes;
    }

  protected ScriptEvaluator getEvaluator( Class returnType, String[] parameterNames, Class[] parameterTypes )
    {
    try
      {
      return new ScriptEvaluator( block, returnType, parameterNames, parameterTypes );
      }
    catch( CompileException exception )
      {
      throw new OperationException( "could not compile script: " + block, exception );
      }
    }

  private Fields makeFields( String[] parameters )
    {
    Comparable[] fields = new Comparable[ parameters.length ];

    for( int i = 0; i < parameters.length; i++ )
      {
      String parameter = parameters[ i ];

      if( parameter.startsWith( "$" ) )
        fields[ i ] = Integer.parseInt( parameter.substring( 1 ) );
      else
        fields[ i ] = parameter;
      }

    return new Fields( fields );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    if( operationCall.getContext() == null )
      operationCall.setContext( new Context() );

    Context context = operationCall.getContext();

    context.parameterNames = getParameterNames();
    context.parameterFields = getParameterFields();
    context.parameterTypes = getParameterTypes();
    context.scriptEvaluator = getEvaluator( getReturnType(), context.parameterNames, context.parameterTypes );
    context.tuple = Tuple.size( 1 );
    }

  protected Class getReturnType()
    {
    return returnType;
    }

  /**
   * Performs the actual expression evaluation.
   *
   * @param context
   * @param input   of type TupleEntry @return Comparable
   */
  protected Object evaluate( Context context, TupleEntry input )
    {
    try
      {
      if( context.parameterTypes.length == 0 )
        return context.scriptEvaluator.evaluate( null );

      Tuple parameterTuple = input.selectTuple( context.parameterFields );

      return context.scriptEvaluator.evaluate( Tuples.asArray( parameterTuple, context.parameterTypes ) );
      }
    catch( InvocationTargetException exception )
      {
      throw new OperationException( "could not evaluate expression: " + block, exception.getTargetException() );
      }
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof ExpressionOperation ) )
      return false;
    if( !super.equals( object ) )
      return false;

    ExpressionOperation that = (ExpressionOperation) object;

    if( block != null ? !block.equals( that.block ) : that.block != null )
      return false;
    if( !Arrays.equals( parameterNames, that.parameterNames ) )
      return false;
    if( !Arrays.equals( parameterTypes, that.parameterTypes ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( block != null ? block.hashCode() : 0 );
    result = 31 * result + ( parameterTypes != null ? Arrays.hashCode( parameterTypes ) : 0 );
    result = 31 * result + ( parameterNames != null ? Arrays.hashCode( parameterNames ) : 0 );
    return result;
    }

  public static class Context
    {
    private Class[] parameterTypes;
    private ScriptEvaluator scriptEvaluator;
    private Fields parameterFields;
    private String[] parameterNames;
    protected Tuple tuple;
    }
  }
