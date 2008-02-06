/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import cascading.operation.Function;
import cascading.operation.Operation;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryListIterator;
import cascading.tuple.Tuples;
import org.codehaus.janino.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

/**
 * Class ExpressionFunction dynamically resolves a given expression using argument {@link Tuple} values. This {@link Function}
 * is based on the <a href="http://www.janino.net/">Janino</a> compiler.
 * <p/>
 * Specifially this function uses the {@link ExpressionEvaluator}, thus the syntax from that class is inherited here.
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
public class ExpressionFunction extends Operation implements Function
  {
  /** Field expression */
  private String expression;
  /** Field parameterTypes */
  private Class[] parameterTypes;

  /** Field expressionEvaluator */
  private transient ExpressionEvaluator expressionEvaluator;
  /** Field parameterFields */
  private transient Fields parameterFields;
  /** Field parameterNames */
  private String[] parameterNames;

  /**
   * Constructor ExpressionFunction creates a new ExpressionFunction instance.
   *
   * @param fieldDeclaration of type Fields
   * @param expression       of type String
   * @param parameterTypes   of type Class[]
   */
  public ExpressionFunction( Fields fieldDeclaration, String expression, Class... parameterTypes )
    {
    super( parameterTypes.length, fieldDeclaration );
    this.expression = expression;
    this.parameterTypes = parameterTypes;

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field, was " + fieldDeclaration.print() );
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
   * Exposed for testing. Performs the actual expression evaluation.
   *
   * @param input of type TupleEntry
   * @return Comparable
   */
  Comparable evaluate( TupleEntry input )
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

  /** @see Function#operate(TupleEntry, TupleEntryListIterator) */
  public void operate( TupleEntry input, TupleEntryListIterator outputCollector )
    {
    outputCollector.add( new Tuple( evaluate( input ) ) );
    }

  }
