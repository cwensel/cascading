/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe;

import java.beans.ConstructorProperties;
import java.util.Set;

import cascading.flow.planner.Scope;
import cascading.operation.Aggregator;
import cascading.operation.AssertionLevel;
import cascading.operation.Buffer;
import cascading.operation.GroupAssertion;
import cascading.tuple.Fields;

/**
 * The Every operator applies an {@link Aggregator} or {@link Buffer} to every grouping.
 * <p/>
 * Any number of Every instances may follow other Every, {@link GroupBy}, or {@link CoGroup} instances if they apply an
 * Aggregator, not a Buffer. If a Buffer, only one Every may follow a GroupBy or CoGroup.
 * <p/>
 * Every operators create aggregate values for every grouping they encounter. This aggregate value is added to the current
 * grouping Tuple.
 * <p/>
 * In the case of a CoGroup, the grouping Tuple will be all the grouping keys from all joined streams,
 * and if an "outer" type join is used, one value on the groupingTuple may be null.
 * <p/>
 * Subsequent Every instances can continue to append values to the grouping Tuple. When an Each follows
 * and Every, the Each applies its operation to the grouping Tuple (thus all child values in the grouping are discarded
 * and only aggregate values are propagated).
 */
public class Every extends Operator
  {
  /** Field AGGREGATOR_ARGUMENTS */
  private static final Fields AGGREGATOR_ARGUMENTS = Fields.ALL;
  /** Field AGGREGATOR_SELECTOR */
  private static final Fields AGGREGATOR_SELECTOR = Fields.ALL;
  /** Field ASSERTION_SELECTOR */
  private static final Fields ASSERTION_SELECTOR = Fields.RESULTS;

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous   previous Pipe to receive input Tuples from
   * @param aggregator Aggregator to be applied to every input Tuple grouping
   */
  @ConstructorProperties({"previous", "aggregator"})
  public Every( Pipe previous, Aggregator aggregator )
    {
    super( previous, AGGREGATOR_ARGUMENTS, aggregator, AGGREGATOR_SELECTOR );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param aggregator       Aggregator to be applied to every input Tuple grouping
   */
  @ConstructorProperties({"previous", "argumentSelector", "aggregator"})
  public Every( Pipe previous, Fields argumentSelector, Aggregator aggregator )
    {
    super( previous, argumentSelector, aggregator, AGGREGATOR_SELECTOR );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param aggregator       Aggregator to be applied to every input Tuple grouping
   * @param outputSelector   field selector that selects the output Tuple from the grouping and Aggregator results Tuples
   */
  @ConstructorProperties({"previous", "argumentSelector", "aggregator", "outputSelector"})
  public Every( Pipe previous, Fields argumentSelector, Aggregator aggregator, Fields outputSelector )
    {
    super( previous, argumentSelector, aggregator, outputSelector );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous       previous Pipe to receive input Tuples from
   * @param aggregator     Aggregator to be applied to every input Tuple grouping
   * @param outputSelector field selector that selects the output Tuple from the grouping and Aggregator results Tuples
   */
  @ConstructorProperties({"previous", "aggregator", "outputSelector"})
  public Every( Pipe previous, Aggregator aggregator, Fields outputSelector )
    {
    super( previous, AGGREGATOR_ARGUMENTS, aggregator, outputSelector );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous previous Pipe to receive input Tuples from
   * @param buffer   Buffer to be applied to every input Tuple grouping
   */
  @ConstructorProperties({"previous", "buffer"})
  public Every( Pipe previous, Buffer buffer )
    {
    super( previous, AGGREGATOR_ARGUMENTS, buffer, AGGREGATOR_SELECTOR );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param buffer           Buffer to be applied to every input Tuple grouping
   */
  @ConstructorProperties({"previous", "argumentSelector", "buffer"})
  public Every( Pipe previous, Fields argumentSelector, Buffer buffer )
    {
    super( previous, argumentSelector, buffer, AGGREGATOR_SELECTOR );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param buffer           Buffer to be applied to every input Tuple grouping
   * @param outputSelector   field selector that selects the output Tuple from the grouping and Buffer results Tuples
   */
  @ConstructorProperties({"previous", "argumentSelector", "buffer", "outputSelector"})
  public Every( Pipe previous, Fields argumentSelector, Buffer buffer, Fields outputSelector )
    {
    super( previous, argumentSelector, buffer, outputSelector );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous       previous Pipe to receive input Tuples from
   * @param buffer         Buffer to be applied to every input Tuple grouping
   * @param outputSelector field selector that selects the output Tuple from the grouping and Buffer results Tuples
   */
  @ConstructorProperties({"previous", "buffer", "outputSelector"})
  public Every( Pipe previous, Buffer buffer, Fields outputSelector )
    {
    super( previous, AGGREGATOR_ARGUMENTS, buffer, outputSelector );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous       previous Pipe to receive input Tuples from
   * @param assertionLevel of type AssertionLevel
   * @param assertion      GroupAssertion to be applied to every input Tuple grouping
   */
  @ConstructorProperties({"previous", "assertionLevel", "assertion"})
  public Every( Pipe previous, AssertionLevel assertionLevel, GroupAssertion assertion )
    {
    super( previous, AGGREGATOR_ARGUMENTS, assertionLevel, assertion, ASSERTION_SELECTOR );
    }

  /**
   * Constructor Every creates a new Every instance.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param assertionLevel   AssertionLevel to associate with the Assertion
   * @param assertion        GroupAssertion to be applied to every input Tuple grouping
   */
  @ConstructorProperties({"previous", "argumentSelector", "assertionLevel", "assertion"})
  public Every( Pipe previous, Fields argumentSelector, AssertionLevel assertionLevel, GroupAssertion assertion )
    {
    super( previous, argumentSelector, assertionLevel, assertion, ASSERTION_SELECTOR );
    }

  /**
   * Method isBuffer returns true if this Every instance holds a {@link cascading.operation.Buffer} operation.
   *
   * @return boolean
   */
  public boolean isBuffer()
    {
    return operation instanceof Buffer;
    }

  /**
   * Method isReducer returns true if this Every instance holds a {@link Aggregator} operation.
   *
   * @return boolean
   */
  public boolean isAggregator()
    {
    return operation instanceof Aggregator;
    }

  public boolean isGroupAssertion()
    {
    return operation instanceof GroupAssertion;
    }

  public Aggregator getAggregator()
    {
    return (Aggregator) operation;
    }

  public Buffer getBuffer()
    {
    return (Buffer) operation;
    }

  public GroupAssertion getGroupAssertion()
    {
    return (GroupAssertion) operation;
    }

  @Override
  public Fields resolveIncomingOperationArgumentFields( Scope incomingScope )
    {
    if( isBuffer() )
      return incomingScope.getIncomingBufferArgumentFields();
    else
      return incomingScope.getIncomingAggregatorArgumentFields();
    }

  @Override
  public Fields resolveIncomingOperationPassThroughFields( Scope incomingScope )
    {
    if( isBuffer() )
      return incomingScope.getIncomingBufferPassThroughFields();
    else
      return incomingScope.getIncomingAggregatorPassThroughFields();
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    Scope incomingScope = getFirst( incomingScopes );

    if( !isBuffer() && incomingScope.getOutValuesFields().isNone() )
      throw new OperatorException( this, "only a Buffer may be preceded by a CoGroup declaring Fields.NONE as the join fields" );

    Fields argumentFields = resolveArgumentSelector( incomingScopes );

    verifyArguments( argumentFields );

    // we currently don't support using result from a previous Every in the current Every
    verifyAggregatorArguments( argumentFields, incomingScope );

    Fields declaredFields = resolveDeclared( incomingScopes, argumentFields );

    verifyDeclaredFields( declaredFields );

    Fields outgoingGroupingFields = resolveOutgoingGroupingSelector( incomingScopes, argumentFields, declaredFields );

    verifyOutputSelector( outgoingGroupingFields );

    Fields outgoingValuesFields = incomingScope.getOutValuesFields();

    // the incoming fields eligible to be outgoing, for Every only the grouping fields.
    Fields passThroughFields = resolveIncomingOperationPassThroughFields( incomingScope );
    Fields remainderFields = resolveRemainderFields( incomingScopes, argumentFields );

    return new Scope( getName(), Scope.Kind.EVERY, passThroughFields, remainderFields, argumentFields, declaredFields, outgoingGroupingFields, outgoingValuesFields );
    }

  private void verifyAggregatorArguments( Fields argumentFields, Scope incomingScope )
    {
    if( ( !isBuffer() ) && incomingScope.isEvery() && argumentFields.contains( incomingScope.getOperationDeclaredFields() ) )
      throw new OperatorException( this, "arguments may not select a declared field from a previous Every" );
    }

  Fields resolveOutgoingGroupingSelector( Set<Scope> incomingScopes, Fields argumentSelector, Fields declared )
    {
    try
      {
      return resolveOutgoingSelector( incomingScopes, argumentSelector, declared );
      }
    catch( Exception exception )
      {
      if( exception instanceof OperatorException )
        throw (OperatorException) exception;

      if( isBuffer() )
        throw new OperatorException( this, "could not resolve outgoing values selector in: " + this, exception );
      else
        throw new OperatorException( this, "could not resolve outgoing grouping selector in: " + this, exception );
      }
    }
  }
