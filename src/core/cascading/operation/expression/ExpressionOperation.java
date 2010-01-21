/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.io.StringReader;
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
import org.codehaus.janino.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

/**
 * Class ExpressionOperation is the base class for {@link ExpressionFunction}, {@link ExpressionFilter},
 * {@link cascading.operation.assertion.AssertExpression}.
 */
public class ExpressionOperation extends BaseOperation<ExpressionOperation.Context>
  {
  /** Field expression */
  protected String expression;

  /** Field parameterTypes */
  private Class[] parameterTypes;
  /** Field parameterNames */
  private String[] parameterNames;

  public static class Context
    {
    private Class[] parameterTypes;
    private ExpressionEvaluator expressionEvaluator;
    private Fields parameterFields;
    private String[] parameterNames;
    }

  @ConstructorProperties({"fieldDeclaration", "expression"})
  protected ExpressionOperation( Fields fieldDeclaration, String expression )
    {
    super( fieldDeclaration );
    this.parameterTypes = new Class[]{};
    this.expression = expression;
    }

  @ConstructorProperties({"fieldDeclaration", "expression", "parameterType"})
  protected ExpressionOperation( Fields fieldDeclaration, String expression, Class parameterType )
    {
    super( fieldDeclaration );
    this.parameterTypes = new Class[]{parameterType};
    this.expression = expression;
    }

  @ConstructorProperties({"fieldDeclaration", "expression", "parameterNames", "parameterTypes"})
  protected ExpressionOperation( Fields fieldDeclaration, String expression, String[] parameterNames, Class[] parameterTypes )
    {
    super( parameterTypes.length, fieldDeclaration );
    this.parameterTypes = Arrays.copyOf( parameterTypes, parameterTypes.length );
    this.parameterNames = Arrays.copyOf( parameterNames, parameterNames.length );
    this.expression = expression;

    if( parameterNames.length != parameterTypes.length )
      throw new IllegalArgumentException( "parameterNames must be same length as parameterTypes" );
    }

  @ConstructorProperties({"fieldDeclaration", "parameterType"})
  protected ExpressionOperation( String expression, Class parameterType )
    {
    this.parameterTypes = new Class[]{parameterType};
    this.expression = expression;
    }

  @ConstructorProperties({"expression", "parameterNames", "parameterTypes"})
  protected ExpressionOperation( String expression, String[] parameterNames, Class[] parameterTypes )
    {
    super( parameterTypes.length );
    this.parameterTypes = Arrays.copyOf( parameterTypes, parameterTypes.length );
    this.parameterNames = Arrays.copyOf( parameterNames, parameterNames.length );
    this.expression = expression;

    if( parameterNames.length != parameterTypes.length )
      throw new IllegalArgumentException( "parameterNames must be same length as parameterTypes" );
    }

  private String[] getParameterNames()
    {
    if( parameterNames != null )
      return parameterNames;

    try
      {
      parameterNames = ExpressionEvaluator.guessParameterNames( new Scanner( "expressionEval", new StringReader( expression ) ) );
      }
    catch( Parser.ParseException exception )
      {
      throw new OperationException( "could not parse expression: " + expression, exception );
      }
    catch( Scanner.ScanException exception )
      {
      throw new OperationException( "could not scan expression: " + expression, exception );
      }
    catch( IOException exception )
      {
      throw new OperationException( "could not read expression: " + expression, exception );
      }

    return parameterNames;
    }

  private Fields getParameterFields()
    {
    return makeFields( getParameterNames() );
    }

  private Class[] getParameterTypes( String[] parameterNames )
    {
    if( parameterNames.length == parameterTypes.length )
      return parameterTypes;

    if( parameterTypes.length != 1 )
      throw new IllegalStateException( "wrong number of parameter types, expects: " + parameterNames.length );

    Class[] types = new Class[parameterNames.length];

    Arrays.fill( types, parameterTypes[ 0 ] );

    parameterTypes = types;

    return parameterTypes;
    }

  private ExpressionEvaluator getExpressionEvaluator( String[] parameterNames, Class[] parameterTypes )
    {
    try
      {
      return new ExpressionEvaluator( expression, Comparable.class, parameterNames, parameterTypes );
      }
    catch( CompileException exception )
      {
      throw new OperationException( "could not compile expression: " + expression, exception );
      }
    catch( Parser.ParseException exception )
      {
      throw new OperationException( "could not parse expression: " + expression, exception );
      }
    catch( Scanner.ScanException exception )
      {
      throw new OperationException( "could not scan expression: " + expression, exception );
      }
    }

  private Fields makeFields( String[] parameters )
    {
    Comparable[] fields = new Comparable[parameters.length];

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
    context.parameterTypes = getParameterTypes( context.parameterNames );
    context.expressionEvaluator = getExpressionEvaluator( context.parameterNames, context.parameterTypes );
    }

  /**
   * Performs the actual expression evaluation.
   *
   * @param context
   * @param input   of type TupleEntry @return Comparable
   */
  protected Comparable evaluate( Context context, TupleEntry input )
    {
    try
      {
      if( context.parameterTypes.length == 0 )
        return (Comparable) context.expressionEvaluator.evaluate( null );

      Tuple parameterTuple = input.selectTuple( context.parameterFields );

      return (Comparable) context.expressionEvaluator.evaluate( Tuples.asArray( parameterTuple, context.parameterTypes ) );
      }
    catch( InvocationTargetException exception )
      {
      throw new OperationException( "could not evaluate expression: " + expression, exception );
      }
    }
  }
