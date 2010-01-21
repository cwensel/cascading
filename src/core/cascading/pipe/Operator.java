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

package cascading.pipe;

import java.util.Set;

import cascading.flow.Scope;
import cascading.operation.Assertion;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Operation;
import cascading.operation.PlannedOperation;
import cascading.operation.PlannerLevel;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleException;

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
  protected PlannerLevel plannerLevel; // do not initialize a default

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

  protected Operator( String name, PlannerLevel plannerLevel, PlannedOperation operation, Fields outputSelector )
    {
    super( name );
    this.plannerLevel = plannerLevel;
    this.operation = operation;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( String name, Fields argumentSelector, PlannerLevel plannerLevel, PlannedOperation operation, Fields outputSelector )
    {
    super( name );
    this.plannerLevel = plannerLevel;
    this.operation = operation;
    this.argumentSelector = argumentSelector;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( Pipe previous, PlannerLevel plannerLevel, PlannedOperation operation, Fields outputSelector )
    {
    super( previous );
    this.plannerLevel = plannerLevel;
    this.operation = operation;
    this.outputSelector = outputSelector;
    verifyOperation();
    }

  protected Operator( Pipe previous, Fields argumentSelector, PlannerLevel plannerLevel, PlannedOperation operation, Fields outputSelector )
    {
    super( previous );
    this.plannerLevel = plannerLevel;
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

    if( operation instanceof PlannedOperation )
      {
      if( plannerLevel == null )
        throw new IllegalArgumentException( "planner level may not be null" );
      else if( plannerLevel.isNoneLevel() )
        throw new IllegalArgumentException( "given planner level: " + plannerLevel.getClass().getName() + ", may not be NONE" );
      }
    }

  /**
   * Method getOperation returns the operation managed by this Operator object.
   *
   * @return the operation (type Operation) of this Operator object.
   */
  public Operation getOperation()
    {
    return operation;
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
  @Deprecated
  public AssertionLevel getAssertionLevel()
    {
    return (AssertionLevel) plannerLevel;
    }

  /**
   * Method getPlannerLevel returns the plannerLevel of this Operator object.
   *
   * @return the plannerLevel (type PlannerLevel) of this Operator object.
   */
  public PlannerLevel getPlannerLevel()
    {
    return plannerLevel;
    }

  /**
   * Method isAssertion returns true if this Operation represents an {@link Assertion} operation.
   *
   * @return the assertion (type boolean) of this Operator object.
   */
  @Deprecated
  public boolean isAssertion()
    {
    return plannerLevel instanceof AssertionLevel;
    }

  /**
   * Method hasPlannerLevel returns true if this Operator object holds a {@link PlannedOperation} object with an associated
   * {@link PlannerLevel} level.
   *
   * @return boolean
   */
  public boolean hasPlannerLevel()
    {
    return plannerLevel != null;
    }

  protected Tuple makeResult( Fields outgoingSelector, TupleEntry inputEntry, Fields remainderFields, TupleEntry declaredEntry, Tuple output )
    {
    if( getOutputSelector().isResults() )
      return output;

    if( getOutputSelector().isAll() )
      return inputEntry.getTuple().append( output );

    if( getOutputSelector().isReplace() )
      {
      Tuple result = new Tuple( inputEntry.getTuple() );

      result.set( inputEntry.getFields(), declaredEntry.getFields(), output );

      return result;
      }

    if( getOutputSelector().isSwap() )
      {
      if( remainderFields.size() == 0 ) // the same as Fields.RESULTS
        return output;
      else
        return inputEntry.selectTuple( remainderFields ).append( output );
      }

    declaredEntry.setTuple( output );

    return TupleEntry.select( outgoingSelector, inputEntry, declaredEntry );
    }

  // FIELDS

  protected Fields resolveRemainderFields( Set<Scope> incomingScopes, Fields argumentFields )
    {
    Fields fields = resolveIncomingOperationFields( getFirst( incomingScopes ) );

    if( fields.isUnknown() )
      return fields;

    return fields.subtract( argumentFields );
    }

  public abstract Scope outgoingScopeFor( Set<Scope> incomingScopes );

  void verifyDeclaredFields( Fields declared )
    {
    if( declared.isDefined() && declared.size() == 0 )
      throw new OperatorException( this, "field declaration: " + getFieldDeclaration().printVerbose() + ", resolves to an empty field set, current grouping is on all fields" );
    }

  void verifyOutputSelector( Fields outputSelector )
    {
    if( outputSelector.isDefined() && outputSelector.size() == 0 )
      throw new OperatorException( this, "output selector: " + getOutputSelector().printVerbose() + ", resolves to an empty field set, current grouping is on all fields" );
    }

  void verifyArguments( Fields argumentSelector )
    {
    if( argumentSelector.isUnknown() )
      return;

    if( operation.getNumArgs() != Operation.ANY && argumentSelector.size() < operation.getNumArgs() )
      throw new OperatorException( this, "resolved wrong number of arguments: " + argumentSelector.printVerbose() + ", expected: " + operation.getNumArgs() );
    }

  Fields resolveOutgoingSelector( Set<Scope> incomingScopes, Fields argumentFields, Fields declaredFields )
    {
    Scope incomingScope = getFirst( incomingScopes );
    Fields outputSelector = getOutputSelector();

    if( outputSelector.isResults() )
      return declaredFields;

    if( outputSelector.isArguments() )
      return argumentFields;

    if( outputSelector.isGroup() )
      return incomingScope.getOutGroupingFields();

    if( outputSelector.isValues() )
      return incomingScope.getOutValuesFields().subtract( incomingScope.getOutGroupingFields() );

    if( outputSelector.isSwap() )
      return incomingScope.getOutValuesFields().subtract( argumentFields ).append( declaredFields );

    Fields incomingFields = resolveFields( incomingScope );

    try
      {
      return Fields.resolve( outputSelector, incomingFields, declaredFields );
      }
    catch( TupleException exception )
      {
      throw new OperatorException( this, incomingFields, declaredFields, outputSelector, exception );
      }
    }

  Fields resolveArgumentSelector( Set<Scope> incomingScopes )
    {
    Fields argumentSelector = getArgumentSelector();

    try
      {
      Scope incomingScope = getFirst( incomingScopes );

      if( argumentSelector.isAll() )
        return resolveIncomingOperationFields( incomingScope );

      if( argumentSelector.isGroup() )
        return incomingScope.getOutGroupingFields();

      if( argumentSelector.isValues() )
        return incomingScope.getOutValuesFields().subtract( incomingScope.getOutGroupingFields() );

      return resolveIncomingOperationFields( incomingScope ).select( argumentSelector );
      }
    catch( FieldsResolverException exception )
      {
      throw new OperatorException( this, OperatorException.Kind.argument, exception.getSourceFields(), argumentSelector, exception );
      }
    catch( Exception exception )
      {
      throw new OperatorException( this, "unable to resolve argument selector: " + argumentSelector.printVerbose(), exception );
      }
    }

  Fields resolveDeclared( Set<Scope> incomingScopes, Fields arguments )
    {
    Fields fieldDeclaration = getFieldDeclaration();

    try
      {
      Scope incomingScope = getFirst( incomingScopes );

      if( fieldDeclaration.isUnknown() )
        return fieldDeclaration;

      if( fieldDeclaration.isArguments() )
        return Fields.asDeclaration( arguments );

      if( fieldDeclaration.isAll() )
        return resolveFields( incomingScope );

      if( fieldDeclaration.isGroup() )
        return incomingScope.getOutGroupingFields();

      // VALUES is the diff between all fields and group fields
      if( fieldDeclaration.isValues() )
        return incomingScope.getOutValuesFields().subtract( incomingScope.getOutGroupingFields() );

      }
    catch( Exception exception )
      {
      throw new OperatorException( this, "could not resolve declared fields in:  " + this, exception );
      }

    if( getOutputSelector().isReplace() )
      {
      if( arguments.isDefined() && fieldDeclaration.isDefined() && arguments.size() != fieldDeclaration.size() )
        throw new OperatorException( this, "during REPLACE both the arguments selector and field declaration must be the same size, arguments: " + arguments.printVerbose() + " declaration: " + fieldDeclaration.printVerbose() );

      return arguments.project( fieldDeclaration );
      }

    return fieldDeclaration;
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