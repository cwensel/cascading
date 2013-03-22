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

package cascading.operation;

import java.io.Serializable;

import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Util;

/**
 * Class BaseOperation is the base class of predicate types that are applied to {@link Tuple} streams via
 * the {@link Each} or {@link Every} {@link Pipe}.
 * </p>
 * Specific examples of Operations are {@link Function}, {@link Filter}, {@link Aggregator}, {@link Buffer},
 * and {@link Assertion}.
 * <p/>
 * By default, {@link #isSafe()} returns {@code true}.
 */
public abstract class BaseOperation<Context> implements Serializable, Operation<Context>
  {
  /** Field fieldDeclaration */
  protected Fields fieldDeclaration;
  /** Field numArgs */
  protected int numArgs = ANY;
  /** Field trace */
  protected final String trace = Util.captureDebugTrace( getClass() );

  // initialize this operation based on its subclass

  {
  if( this instanceof Filter || this instanceof Assertion )
    fieldDeclaration = Fields.ALL;
  else
    fieldDeclaration = Fields.UNKNOWN;
  }

  /**
   * Constructs a new instance that returns an {@link Fields#UNKNOWN} {@link Tuple} and accepts any number of arguments.
   * </p>
   * It is a best practice to always declare the field names and number of arguments via one of the other constructors.
   */
  protected BaseOperation()
    {
    }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts any number of arguments.
   *
   * @param fieldDeclaration of type Fields
   */
  protected BaseOperation( Fields fieldDeclaration )
    {
    this.fieldDeclaration = fieldDeclaration;
    verify();
    }

  /**
   * Constructs a new instance that returns an unknown field set and accepts the given numArgs number of arguments.
   *
   * @param numArgs of type numArgs
   */
  protected BaseOperation( int numArgs )
    {
    this.numArgs = numArgs;
    verify();
    }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts numArgs number of arguments.
   *
   * @param numArgs          of type numArgs
   * @param fieldDeclaration of type Fields
   */
  protected BaseOperation( int numArgs, Fields fieldDeclaration )
    {
    this.numArgs = numArgs;
    this.fieldDeclaration = fieldDeclaration;
    verify();
    }

  /** Validates the state of this instance. */
  private final void verify()
    {
    if( this instanceof Filter && fieldDeclaration != Fields.ALL )
      throw new IllegalArgumentException( "fieldDeclaration must be set to Fields.ALL for filter operations" );

    if( this instanceof Assertion && fieldDeclaration != Fields.ALL )
      throw new IllegalArgumentException( "fieldDeclaration must be set to Fields.ALL for assertion operations" );

    if( fieldDeclaration == null )
      throw new IllegalArgumentException( "fieldDeclaration may not be null" );

    if( numArgs < 0 )
      throw new IllegalArgumentException( "numArgs may not be negative" );
    }

  /** Method prepare does nothing, and may safely be overridden. */
  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    // do nothing
    }

  @Override
  public void flush( FlowProcess flowProcess, OperationCall<Context> contextOperationCall )
    {
    }

  /** Method cleanup does nothing, and may safely be overridden. */
  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    // do nothing
    }

  @Override
  public Fields getFieldDeclaration()
    {
    return fieldDeclaration;
    }

  @Override
  public int getNumArgs()
    {
    return numArgs;
    }

  @Override
  public boolean isSafe()
    {
    return true;
    }

  /**
   * Method getTrace returns the trace of this BaseOperation object.
   *
   * @return the trace (type String) of this BaseOperation object.
   */
  public String getTrace()
    {
    return trace;
    }

  @Override
  public String toString()
    {
    return toStringInternal( (Operation) this );
    }

  public static String toStringInternal( Operation operation )
    {
    StringBuilder buffer = new StringBuilder();

    Class<? extends Operation> type = operation.getClass();
    if( type.getSimpleName().length() != 0 )
      buffer.append( type.getSimpleName() );
    else
      buffer.append( type.getName() ); // should get something for an anonymous inner class

    if( operation.getFieldDeclaration() != null )
      buffer.append( "[decl:" ).append( operation.getFieldDeclaration() ).append( "]" );

    if( operation.getNumArgs() != ANY )
      buffer.append( "[args:" ).append( operation.getNumArgs() ).append( "]" );

    return buffer.toString();
    }

  public static void printOperationInternal( Operation operation, StringBuffer buffer, Scope scope )
    {
    Class<? extends Operation> type = operation.getClass();

    if( type.getSimpleName().length() != 0 )
      buffer.append( type.getSimpleName() );
    else
      buffer.append( type.getName() ); // should get something for an anonymous inner class

    if( scope.getOperationDeclaredFields() != null )
      buffer.append( "[decl:" ).append( scope.getOperationDeclaredFields().printVerbose() ).append( "]" );

    if( operation.getNumArgs() != ANY )
      buffer.append( "[args:" ).append( operation.getNumArgs() ).append( "]" );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof BaseOperation ) )
      return false;

    BaseOperation that = (BaseOperation) object;

    if( numArgs != that.numArgs )
      return false;
    if( fieldDeclaration != null ? !fieldDeclaration.equals( that.fieldDeclaration ) : that.fieldDeclaration != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = fieldDeclaration != null ? fieldDeclaration.hashCode() : 0;
    result = 31 * result + numArgs;
    return result;
    }
  }