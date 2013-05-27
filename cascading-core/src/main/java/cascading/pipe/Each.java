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

package cascading.pipe;

import java.beans.ConstructorProperties;
import java.util.Set;

import cascading.flow.planner.Scope;
import cascading.operation.Assertion;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.ValueAssertion;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * The Each operator applies either a {@link Function} or a {@link Filter} to each entry in the {@link Tuple}
 * stream. Any number of Each operators can follow an Each, {@link Splice}, or {@link Every}
 * operator.
 */
public class Each extends Operator
  {
  /** Field FUNCTION_SELECTOR */
  private static final Fields FUNCTION_SELECTOR = Fields.RESULTS;
  /** Field FILTER_SELECTOR */
  private static final Fields FILTER_SELECTOR = Fields.RESULTS;

  ///////////////////
  // TAKE FUNCTIONS
  ///////////////////

  /**
   * Pass all fields to the given function, only return fields declared by the function.
   *
   * @param name     name for this branch of Pipes
   * @param function Function to be applied to each input Tuple
   */
  @ConstructorProperties({"name", "function"})
  public Each( String name, Function function )
    {
    super( name, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass argumentFields to the given function, only return fields declared by the function.
   *
   * @param name             name for this branch of Pipes
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param function         Function to be applied to each input Tuple
   */
  @ConstructorProperties({"name", "argumentSelector", "function"})
  public Each( String name, Fields argumentSelector, Function function )
    {
    super( name, argumentSelector, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass argumentFields to the given function, only return fields selected by the outputSelector.
   *
   * @param name             name for this branch of Pipes
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param function         Function to be applied to each input Tuple
   * @param outputSelector   field selector that selects the output Tuple from the input and Function results Tuples
   */
  @ConstructorProperties({"name", "argumentSelector", "function", "outputSelector"})
  public Each( String name, Fields argumentSelector, Function function, Fields outputSelector )
    {
    super( name, argumentSelector, function, outputSelector );
    }

  /**
   * Only return fields selected by the outputSelector.
   *
   * @param name           name for this branch of Pipes
   * @param function       Function to be applied to each input Tuple
   * @param outputSelector field selector that selects the output Tuple from the input and Function results Tuples
   */
  @ConstructorProperties({"name", "function", "outputSelector"})
  public Each( String name, Function function, Fields outputSelector )
    {
    super( name, function, outputSelector );
    }

  /**
   * Pass all fields to the given function, only return fields declared by the function.
   *
   * @param previous previous Pipe to receive input Tuples from
   * @param function Function to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "function"})
  public Each( Pipe previous, Function function )
    {
    super( previous, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass argumentFields to the given function, only return fields declared by the function.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param function         Function to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "argumentSelector", "function"})
  public Each( Pipe previous, Fields argumentSelector, Function function )
    {
    super( previous, argumentSelector, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass argumentFields to the given function, only return fields selected by the outputSelector.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param function         Function to be applied to each input Tuple
   * @param outputSelector   field selector that selects the output Tuple from the input and Function results Tuples
   */
  @ConstructorProperties({"previous", "argumentSelector", "function", "outputSelector"})
  public Each( Pipe previous, Fields argumentSelector, Function function, Fields outputSelector )
    {
    super( previous, argumentSelector, function, outputSelector );
    }

  /**
   * Only pass argumentFields to the given function, only return fields selected by the outputSelector.
   *
   * @param previous       previous Pipe to receive input Tuples from
   * @param function       Function to be applied to each input Tuple
   * @param outputSelector field selector that selects the output Tuple from the input and Function results Tuples
   */
  @ConstructorProperties({"previous", "function", "outputSelector"})
  public Each( Pipe previous, Function function, Fields outputSelector )
    {
    super( previous, function, outputSelector );
    }

  /////////////////
  // TAKE FILTERS
  /////////////////

  /**
   * Constructor Each creates a new Each instance.
   *
   * @param name   name for this branch of Pipes
   * @param filter Filter to be applied to each input Tuple
   */
  @ConstructorProperties({"name", "filter"})
  public Each( String name, Filter filter )
    {
    super( name, filter, FILTER_SELECTOR );
    }

  /**
   * Constructor Each creates a new Each instance.
   *
   * @param name             name for this branch of Pipes
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param filter           Filter to be applied to each input Tuple
   */
  @ConstructorProperties({"name", "argumentSelector", "filter"})
  public Each( String name, Fields argumentSelector, Filter filter )
    {
    super( name, argumentSelector, filter, FILTER_SELECTOR );
    }

  /**
   * Constructor Each creates a new Each instance.
   *
   * @param previous previous Pipe to receive input Tuples from
   * @param filter   Filter to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "filter"})
  public Each( Pipe previous, Filter filter )
    {
    super( previous, filter, FILTER_SELECTOR );
    }

  /**
   * Constructor Each creates a new Each instance.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param filter           Filter to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "argumentSelector", "filter"})
  public Each( Pipe previous, Fields argumentSelector, Filter filter )
    {
    super( previous, argumentSelector, filter, FILTER_SELECTOR );
    }

  ///////////////
  // ASSERTIONS
  ///////////////

  /**
   * Constructor Each creates a new Each instance.
   *
   * @param name           name for this branch of Pipes
   * @param assertionLevel AssertionLevel to associate with the Assertion
   * @param assertion      Assertion to be applied to each input Tuple
   */
  @ConstructorProperties({"name", "assertionLevel", "assertion"})
  public Each( String name, AssertionLevel assertionLevel, Assertion assertion )
    {
    super( name, assertionLevel, assertion, FILTER_SELECTOR );
    }

  /**
   * @param name             name for this branch of Pipes
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param assertionLevel   AssertionLevel to associate with the Assertion
   * @param assertion        Assertion to be applied to each input Tuple
   */
  @ConstructorProperties({"name", "argumentSelector", "assertionLevel", "assertion"})
  public Each( String name, Fields argumentSelector, AssertionLevel assertionLevel, Assertion assertion )
    {
    super( name, argumentSelector, assertionLevel, assertion, FILTER_SELECTOR );
    }

  /**
   * @param previous       previous Pipe to receive input Tuples from
   * @param assertionLevel AssertionLevel to associate with the Assertion
   * @param assertion      Assertion to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "assertionLevel", "assertion"})
  public Each( Pipe previous, AssertionLevel assertionLevel, Assertion assertion )
    {
    super( previous, assertionLevel, assertion, FILTER_SELECTOR );
    }

  /**
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param assertionLevel   AssertionLevel to associate with the Assertion
   * @param assertion        Assertion to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "argumentSelector", "assertionLevel", "assertion"})
  public Each( Pipe previous, Fields argumentSelector, AssertionLevel assertionLevel, Assertion assertion )
    {
    super( previous, argumentSelector, assertionLevel, assertion, FILTER_SELECTOR );
    }

  //////////
  //DEBUG
  //////////

  /**
   * @param name             name for this branch of Pipes
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param debugLevel       DebugLevel to associate with the Debug
   * @param debug            Debug to be applied to each input Tuple
   */
  @ConstructorProperties({"name", "argumentSelector", "debugLevel", "debug"})
  public Each( String name, Fields argumentSelector, DebugLevel debugLevel, Debug debug )
    {
    super( name, argumentSelector, debugLevel, debug, FILTER_SELECTOR );
    }

  /**
   * @param previous   previous Pipe to receive input Tuples from
   * @param debugLevel DebugLevel to associate with the Debug
   * @param debug      Debug to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "debugLevel", "debug"})
  public Each( Pipe previous, DebugLevel debugLevel, Debug debug )
    {
    super( previous, debugLevel, debug, FILTER_SELECTOR );
    }

  /**
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param debugLevel       DebugLevel to associate with the Debug
   * @param debug            Debug to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "argumentSelector", "debugLevel", "debug"})
  public Each( Pipe previous, Fields argumentSelector, DebugLevel debugLevel, Debug debug )
    {
    super( previous, argumentSelector, debugLevel, debug, FILTER_SELECTOR );
    }

  @Override
  protected void verifyOperation()
    {
    // backwards compatibility with 1.0
    if( plannerLevel == null && operation instanceof Debug )
      plannerLevel = DebugLevel.DEFAULT;

    super.verifyOperation();

    if( !argumentSelector.isArgSelector() )
      throw new IllegalArgumentException( "invalid argument selector: " + argumentSelector );

    if( !operation.getFieldDeclaration().isDeclarator() )
      throw new IllegalArgumentException( "invalid field declaration: " + operation.getFieldDeclaration() );

    if( !outputSelector.isOutSelector() )
      throw new IllegalArgumentException( "invalid output selector: " + outputSelector );
    }

  public Function getFunction()
    {
    return (Function) operation;
    }

  public Filter getFilter()
    {
    return (Filter) operation;
    }

  public ValueAssertion getValueAssertion()
    {
    return (ValueAssertion) operation;
    }

  public boolean isFunction()
    {
    return operation instanceof Function;
    }

  public boolean isFilter()
    {
    return operation instanceof Filter;
    }

  public boolean isValueAssertion()
    {
    return operation instanceof ValueAssertion;
    }

  // FIELDS

  @Override
  public Fields resolveIncomingOperationArgumentFields( Scope incomingScope )
    {
    return incomingScope.getIncomingFunctionArgumentFields();
    }

  @Override
  public Fields resolveIncomingOperationPassThroughFields( Scope incomingScope )
    {
    return incomingScope.getIncomingFunctionPassThroughFields();
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    Fields argumentFields = resolveArgumentSelector( incomingScopes );

    verifyArguments( argumentFields );

    Fields declaredFields = resolveDeclared( incomingScopes, argumentFields );

    verifyDeclaredFields( declaredFields );

    Fields outgoingValuesFields = resolveOutgoingValuesSelector( incomingScopes, argumentFields, declaredFields );

    verifyOutputSelector( outgoingValuesFields );

    Fields outgoingGroupingFields = Fields.asDeclaration( outgoingValuesFields );

    // the incoming fields eligible to be outgoing
    Fields passThroughFields = resolveIncomingOperationPassThroughFields( getFirst( incomingScopes ) );
    Fields remainderFields = resolveRemainderFields( incomingScopes, argumentFields );

    return new Scope( getName(), Scope.Kind.EACH, passThroughFields, remainderFields, argumentFields, declaredFields, outgoingGroupingFields, outgoingValuesFields );
    }

  Fields resolveOutgoingValuesSelector( Set<Scope> incomingScopes, Fields argumentFields, Fields declaredFields )
    {
    try
      {
      return resolveOutgoingSelector( incomingScopes, argumentFields, declaredFields );
      }
    catch( Exception exception )
      {
      if( exception instanceof OperatorException )
        throw (OperatorException) exception;

      throw new OperatorException( this, "could not resolve outgoing values selector in: " + this, exception );
      }
    }
  }
