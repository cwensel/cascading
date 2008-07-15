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

package cascading.pipe;

import java.util.Set;

import cascading.flow.Scope;
import cascading.operation.Assertion;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Operation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * An Opererator is a type of {@link Pipe}. Operators pass specified arguments to a given {@link cascading.operation.BaseOperation}.
 * </p>
 * The argFields value select the input fields used by the operation. By default the whole input Tuple is passes as arguments.
 * The outFields value select the fields in the result Tuple returned by this Pipe. By default, the operation results
 * of the given operation replace the input Tuple.
 */
public abstract class Operator extends Pipe
  {
  /** Field operation */
  protected final Operation operation;
  /** Field argumentSelector */
  protected Fields argumentSelector = Fields.ALL; // use wildcard. let the operation choose
  /** Field outputSelector */
  protected Fields outputSelector = Fields.RESULTS;  // this is overridden by the subclasses via the ctor
  /** Field assertionLevel */
  protected AssertionLevel assertionLevel; // do not initialize a default

  protected Operator( Operation operation )
    {
    this.operation = operation;
    verifyOperation();
    }

  protected Operator( String name, Operation operation )
    {
    super( name );
    this.operation = operation;
    verifyOperation();
    }

  protected Operator( String name, Operation operation, Fields outputSelector )
    {
    super( name );
    this.operation = operation;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( String name, Fields argumentSelector, Operation operation )
    {
    super( name );
    this.operation = operation;
    this.argumentSelector = argumentSelector;
    verifyOperation();
    }

  protected Operator( String name, Fields argumentSelector, Operation operation, Fields outputSelector )
    {
    super( name );
    this.operation = operation;
    this.argumentSelector = argumentSelector;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( Pipe previous, Operation operation )
    {
    super( previous );
    this.operation = operation;
    verifyOperation();
    }

  protected Operator( Pipe previous, Fields argumentSelector, Operation operation )
    {
    super( previous );
    this.operation = operation;
    this.argumentSelector = argumentSelector;
    verifyOperation();
    }

  protected Operator( Pipe previous, Fields argumentSelector, Operation operation, Fields outputSelector )
    {
    super( previous );
    this.operation = operation;
    this.argumentSelector = argumentSelector;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( Pipe previous, Operation operation, Fields outputSelector )
    {
    super( previous );
    this.operation = operation;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( String name, AssertionLevel assertionLevel, Operation operation, Fields outputSelector )
    {
    super( name );
    this.assertionLevel = assertionLevel;
    this.operation = operation;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( String name, Fields argumentSelector, AssertionLevel assertionLevel, Operation operation, Fields outputSelector )
    {
    super( name );
    this.assertionLevel = assertionLevel;
    this.operation = operation;
    this.argumentSelector = argumentSelector;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( Pipe previous, AssertionLevel assertionLevel, Operation operation, Fields outputSelector )
    {
    super( previous );
    this.assertionLevel = assertionLevel;
    this.operation = operation;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( Pipe previous, Fields argumentSelector, AssertionLevel assertionLevel, Operation operation, Fields outputSelector )
    {
    super( previous );
    this.assertionLevel = assertionLevel;
    this.operation = operation;
    this.argumentSelector = argumentSelector;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected void verifyOperation()
    {
    if( operation == null )
      throw new IllegalArgumentException( "operation may not be null" );

    if( argumentSelector == null )
      throw new IllegalArgumentException( "argumentSelector may not be null" );

    if( outputSelector == null )
      throw new IllegalArgumentException( "outputSelector may not be null" );

    if( operation instanceof Assertion && ( assertionLevel == null || assertionLevel == AssertionLevel.NONE ) )
      throw new IllegalArgumentException( "assertionLevel may not be null or NONE" );
    }

  /**
   * Method getArgumentSelector returns the argumentSelector of this Operator object.
   *
   * @return the argumentSelector (type Fields) of this Operator object.
   */
  public Fields getArgumentSelector()
    {
    return argumentSelector;
    }

  /**
   * Method getFieldDeclaration returns the fieldDeclaration of this Operator object.
   *
   * @return the fieldDeclaration (type Fields) of this Operator object.
   */
  public Fields getFieldDeclaration()
    {
    return operation.getFieldDeclaration();
    }

  /**
   * Method getOutputSelector returns the outputSelector of this Operator object.
   *
   * @return the outputSelector (type Fields) of this Operator object.
   */
  public Fields getOutputSelector()
    {
    return outputSelector;
    }

  /**
   * Method getAssertionLevel returns the assertionLevel of this Operator object. Only used if the {@link cascading.operation.Operation}
   * is an {@link Assertion}.
   *
   * @return the assertionLevel (type Assertion.Level) of this Operator object.
   */
  public AssertionLevel getAssertionLevel()
    {
    return assertionLevel;
    }

  /**
   * Method isAssertion returns true if this Operation represents an {@link Assertion}.
   *
   * @return the assertion (type boolean) of this Operator object.
   */
  public boolean isAssertion()
    {
    return assertionLevel != null;
    }

  protected Tuple makeResult( Fields outgoingSelector, TupleEntry input, TupleEntry declaredEntry, Tuple output )
    {
    if( getOutputSelector().isResults() )
      return output;

    if( getOutputSelector().isAll() )
      return input.getTuple().append( output );

    declaredEntry.setTuple( output );

    return TupleEntry.select( outgoingSelector, input, declaredEntry );
    }

  // FIELDS

  public abstract Scope outgoingScopeFor( Set<Scope> incomingScopes );

  void verifyDeclared( Fields declared )
    {
    if( declared.isDefined() && declared.size() == 0 )
      throw new OperatorException( "field declaration: " + getFieldDeclaration().print() + ", resolves to an empty field set, current grouping is on all fields" );
    }

  void verifyOutputSelector( Fields outputSelector )
    {
    if( outputSelector.isDefined() && outputSelector.size() == 0 )
      throw new OperatorException( "output selector: " + getOutputSelector().print() + ", resolves to an empty field set, current grouping is on all fields" );
    }

  void verifyArguments( Fields argumentSelector )
    {
    if( operation.getNumArgs() != Operation.ANY && argumentSelector.size() < operation.getNumArgs() )
      throw new OperatorException( "resolved wrong number of arguments: " + argumentSelector.print() + ", expected: " + operation.getNumArgs() );
    }

  Fields resolveOutgoingSelector( Set<Scope> incomingScopes, Fields argumentSelector, Fields declared )
    {
    Scope incomingScope = getFirst( incomingScopes );
    Fields outputSelector = getOutputSelector();

    if( outputSelector.isResults() )
      return declared;

    if( outputSelector.isArguments() )
      return argumentSelector;

    if( outputSelector.isKeys() )
      return incomingScope.getOutGroupingFields();

    if( outputSelector.isValues() )
      return incomingScope.getOutValuesFields().minus( incomingScope.getOutGroupingFields() );

    return Fields.resolve( outputSelector, resolveFields( incomingScope ), declared );
    }

  Fields resolveArgumentSelector( Set<Scope> incomingScopes )
    {
    try
      {
      Scope incomingScope = getFirst( incomingScopes );
      Fields argumentSelector = getArgumentSelector();

      if( argumentSelector.isAll() )
        return resolveIncomingOperationFields( incomingScope );

      if( argumentSelector.isKeys() )
        return incomingScope.getOutGroupingFields();

      if( argumentSelector.isValues() )
        return incomingScope.getOutValuesFields().minus( incomingScope.getOutGroupingFields() );

      return resolveIncomingOperationFields( incomingScope ).select( argumentSelector );
      }
    catch( Exception exception )
      {
      throw new OperatorException( "could not resolve argument selector in: " + this, exception );
      }
    }

  Fields resolveDeclared( Set<Scope> incomingScopes, Fields arguments )
    {
    try
      {
      Scope incomingScope = getFirst( incomingScopes );
      Fields fieldDeclaration = getFieldDeclaration();

      if( fieldDeclaration.isUnknown() )
        return fieldDeclaration;

      if( fieldDeclaration.isArguments() )
        return Fields.asDeclaration( arguments );

      if( fieldDeclaration.isAll() )
        return resolveFields( incomingScope );

      if( fieldDeclaration.isKeys() )
        return incomingScope.getOutGroupingFields();

      // VALUES is the diff between all fields and group fields
      if( fieldDeclaration.isValues() )
        return incomingScope.getOutValuesFields().minus( incomingScope.getOutGroupingFields() );

      return fieldDeclaration;
      }
    catch( Exception exception )
      {
      throw new OperatorException( "could not resolve declared fields in:  " + this, exception );
      }
    }

  // OBJECT OVERRIDES

  @Override
  public String toString()
    {
    return super.toString() + "[" + operation + "]";
    }

  @Override
  protected void printInternal( StringBuffer buffer, Scope scope )
    {
    super.printInternal( buffer, scope );
    buffer.append( "[" );
    BaseOperation.printOperationInternal( operation, buffer, scope );
    buffer.append( "]" );
    }

  @SuppressWarnings({"RedundantIfStatement"})
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    Operator operator = (Operator) object;

    if( argumentSelector != null ? !argumentSelector.equals( operator.argumentSelector ) : operator.argumentSelector != null )
      return false;
    if( operation != null ? !operation.equals( operator.operation ) : operator.operation != null )
      return false;
    if( outputSelector != null ? !outputSelector.equals( operator.outputSelector ) : operator.outputSelector != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( operation != null ? operation.hashCode() : 0 );
    result = 31 * result + ( argumentSelector != null ? argumentSelector.hashCode() : 0 );
    result = 31 * result + ( outputSelector != null ? outputSelector.hashCode() : 0 );
    return result;
    }
  }