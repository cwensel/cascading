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

package cascading.operation;

import java.io.Serializable;

import cascading.flow.Scope;
import cascading.operation.generator.Generator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class Operation is the base class of predicate types that are applied to {@link Tuple} streams via the {@link Each} or {@link Every} {@link Pipe}.
 * Specific examples of Operations are {@link Function}, {@link Filter}, {@link Generator}, and
 * {@link Aggregator}.
 */
public abstract class Operation implements Serializable
  {
  /** Field ANY denotes that a given Operation will take any number of argument values */
  public static final int ANY = Integer.MAX_VALUE;

  /** Field fieldDeclaration */
  protected Fields fieldDeclaration = Fields.UNKNOWN;
  /** Field numArgs */
  protected int numArgs = ANY;

  protected Operation()
    {
    }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts any number of arguments.
   *
   * @param fieldDeclaration of type Fields
   */
  protected Operation( Fields fieldDeclaration )
    {
    this.fieldDeclaration = fieldDeclaration;
    verify();
    }

  /**
   * Constructs a new instance that returns an unknown field set and accepts the given numArgs number of arguments.
   *
   * @param numArgs of type numArgs
   */
  protected Operation( int numArgs )
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
  protected Operation( int numArgs, Fields fieldDeclaration )
    {
    this.numArgs = numArgs;
    this.fieldDeclaration = fieldDeclaration;
    verify();
    }

  /** Validates the state of this instance. */
  private final void verify()
    {
    if( fieldDeclaration == null )
      throw new IllegalArgumentException( "fieldDeclaration may not be null" );

    if( numArgs < 0 )
      throw new IllegalArgumentException( "numArgs may not be negative" );
    }

  /**
   * Method getFieldDeclaration returns the fieldDeclaration of this Operation object.
   *
   * @return the fieldDeclaration (type Fields) of this Operation object.
   */
  public Fields getFieldDeclaration()
    {
    return fieldDeclaration;
    }

  /**
   * Return the number of arguments passed to this operation.
   *
   * @return the number of arguments passed to this operation.
   */
  public int getNumArgs()
    {
    return numArgs;
    }

  @Override
  public String toString()
    {
    StringBuilder buffer = new StringBuilder();

    if( getClass().getSimpleName().length() != 0 )
      buffer.append( getClass().getSimpleName() );
    else
      buffer.append( getClass().getName() ); // should get something for an anonymous inner class

    if( fieldDeclaration != null )
      buffer.append( "[decl:" ).append( fieldDeclaration ).append( "]" );

    if( numArgs != ANY )
      buffer.append( "[args:" ).append( numArgs ).append( "]" );

    return buffer.toString();
    }

  public void printInternal( StringBuffer buffer, Scope scope )
    {
    if( getClass().getSimpleName().length() != 0 )
      buffer.append( getClass().getSimpleName() );
    else
      buffer.append( getClass().getName() ); // should get something for an anonymous inner class

    if( scope.getDeclaredFields() != null )
      buffer.append( "[decl:" ).append( scope.getDeclaredFields() ).append( "]" );

    if( numArgs != ANY )
      buffer.append( "[args:" ).append( numArgs ).append( "]" );

    }
  }