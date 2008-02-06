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

package cascading.pipe;

import java.util.Set;

import cascading.flow.Scope;
import cascading.operation.Operation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * argFields define the input fields used by the function. By default the first field from the input Tuple is used.
 * outFields define the fields in the result Tuple returned by this Pipe. By default, the operation results replace the
 * input Tuple.
 * <p/>
 * A null outFieldSelector denote that the operation replaces the whole input Tuple. Otherwise, the result of the
 * operation are appended to the input Tuple, and any Fields given as outFields are used to prune the Tuple.
 */
public abstract class Operator extends Pipe
  {
  /** Field operation */
  protected final Operation operation;
  /** Field argumentSelector */
  protected Fields argumentSelector = Fields.ALL; // use wildcard. let the operator choose
  /** Field outputSelector */
  protected Fields outputSelector = Fields.RESULTS;  // this is overridden by the subclasses via the ctor

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

  public Operator( Pipe previous, Operation operation )
    {
    super( previous );
    this.operation = operation;
    verifyOperation();
    }

  public Operator( Pipe previous, Fields argumentSelector, Operation operation )
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

  public Operator( Pipe previous, Operation operation, Fields outputSelector )
    {
    super( previous );
    this.operation = operation;
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
    }

  public Fields getArgumentSelector()
    {
    return argumentSelector;
    }

  public Fields getFieldDeclaration()
    {
    return operation.getFieldDeclaration();
    }

  public Fields getOutputSelector()
    {
    return outputSelector;
    }

  protected Tuple makeResult( Fields outgoingSelector, TupleEntry input, TupleEntry output )
    {
    if( getOutputSelector().isResults() )
      return output.getTuple();

    // appendNew checks for duplicate fields and fails accordingly
    if( getOutputSelector().isAll() )
      return input.appendNew( output ).getTuple();

    return TupleEntry.select( outgoingSelector, input, output );
    }

  // FIELDS

  /**
   * Method outgoingScopeFor returns the {@link Scope} being handed to the next pipe element.
   *
   * @param incomingScopes of type Set<Scope>
   * @return Scope
   */
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
    operation.printInternal( buffer, scope );
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

  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( operation != null ? operation.hashCode() : 0 );
    result = 31 * result + ( argumentSelector != null ? argumentSelector.hashCode() : 0 );
    result = 31 * result + ( outputSelector != null ? outputSelector.hashCode() : 0 );
    return result;
    }
  }