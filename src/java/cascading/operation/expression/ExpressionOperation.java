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

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;

import cascading.operation.Operation;
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
 *
 */
public class ExpressionOperation extends Operation
  {
  /** Field expression */
  protected String expression;
  /** Field parameterTypes */
  protected Class[] parameterTypes;
  /** Field expressionEvaluator */
  private transient ExpressionEvaluator expressionEvaluator;
  /** Field parameterFields */
  private transient Fields parameterFields;
  /** Field parameterNames */
  private String[] parameterNames;

  protected ExpressionOperation( int numArgs, Fields fieldDeclaration, String expression, Class... parameterTypes )
    {
    super( numArgs, fieldDeclaration );
    this.parameterTypes = parameterTypes;
    this.expression = expression;
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
    if( parameterFields == null )
      parameterFields = makeFields( getParameterNames() );

    return parameterFields;
    }

  private ExpressionEvaluator getExpressionEvaluator()
    {
    if( expressionEvaluator != null )
      return expressionEvaluator;

    try
      {
      expressionEvaluator = new ExpressionEvaluator( expression, Comparable.class, getParameterNames(), parameterTypes );
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

    return expressionEvaluator;
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

  /**
   * Performs the actual expression evaluation.
   *
   * @param input of type TupleEntry
   * @return Comparable
   */
  protected Comparable evaluate( TupleEntry input )
    {
    Tuple parameterTuple = input.selectTuple( getParameterFields() );

    Comparable value = null;

    try
      {
      value = (Comparable) getExpressionEvaluator().evaluate( Tuples.asArray( parameterTuple, parameterTypes ) );
      }
    catch( InvocationTargetException exception )
      {
      throw new OperationException( "could not evaluate expression: " + expression, exception );
      }

    return value;
    }
  }
