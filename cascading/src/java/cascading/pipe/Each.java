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

import cascading.flow.FlowCollector;
import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.operation.Assertion;
import cascading.operation.AssertionLevel;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.ValueAssertion;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;
import org.apache.log4j.Logger;

/**
 * The Each operator applies either a {@link Function} or a {@link Filter} to each entry in the {@link Tuple}
 * stream. Any number of Each operators can follow an Each, {@link Group}, or {@link Every}
 * operator.
 */
public class Each extends Operator
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( Each.class );
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
   * @param name     of type String
   * @param function of type Function
   */
  public Each( String name, Function function )
    {
    super( name, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass arguementFields to the given function, only return fields declared by the function.
   *
   * @param name                  of type String
   * @param argumentFieldSelector of type Fields
   * @param function              of type Function
   */
  public Each( String name, Fields argumentFieldSelector, Function function )
    {
    super( name, argumentFieldSelector, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass arguementFields to the given function, only return fields selected by the outFieldsSelector.
   *
   * @param name                  of type String
   * @param argumentFieldSelector of type Fields
   * @param function              of type Function
   * @param outFieldSelector      of type Fields
   */
  public Each( String name, Fields argumentFieldSelector, Function function, Fields outFieldSelector )
    {
    super( name, argumentFieldSelector, function, outFieldSelector );
    }

  /**
   * Only return fields selected by the outFieldsSelector.
   *
   * @param name             of type String
   * @param function         of type Function
   * @param outFieldSelector of type Fields
   */
  public Each( String name, Function function, Fields outFieldSelector )
    {
    super( name, function, outFieldSelector );
    }

  /**
   * Pass all fields to the given function, only return fields declared by the function.
   *
   * @param previous of type Pipe
   * @param function of type Function
   */
  public Each( Pipe previous, Function function )
    {
    super( previous, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass arguementFields to the given function, only return fields declared by the function.
   *
   * @param pipe                  of type Pipe
   * @param argumentFieldSelector of type Fields
   * @param function              of type Function
   */
  public Each( Pipe pipe, Fields argumentFieldSelector, Function function )
    {
    super( pipe, argumentFieldSelector, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass arguementFields to the given function, only return fields selected by the outFieldsSelector.
   *
   * @param pipe                  of type Pipe
   * @param argumentFieldSelector of type Fields
   * @param function              of type Function
   * @param outFieldSelector      of type Fields
   */
  public Each( Pipe pipe, Fields argumentFieldSelector, Function function, Fields outFieldSelector )
    {
    super( pipe, argumentFieldSelector, function, outFieldSelector );
    }

  /**
   * Only pass arguementFields to the given function, only return fields selected by the outFieldsSelector.
   *
   * @param pipe             of type Pipe
   * @param function         of type Function
   * @param outFieldSelector of type Fields
   */
  public Each( Pipe pipe, Function function, Fields outFieldSelector )
    {
    super( pipe, function, outFieldSelector );
    }

  /////////////////
  // TAKE FILTERS
  /////////////////

  /**
   * Constructor Each creates a new Each instance.
   *
   * @param name   of type String
   * @param filter of type Filter
   */
  public Each( String name, Filter filter )
    {
    super( name, filter, FILTER_SELECTOR );
    }

  /**
   * @param name                  of type String
   * @param argumentFieldSelector of type Fields
   * @param filter                of type Filter
   */
  public Each( String name, Fields argumentFieldSelector, Filter filter )
    {
    super( name, argumentFieldSelector, filter, FILTER_SELECTOR );
    }

  /**
   * @param previous of type Pipe
   * @param filter   of type Filter
   */
  public Each( Pipe previous, Filter filter )
    {
    super( previous, filter, FILTER_SELECTOR );
    }

  /**
   * @param pipe                  of type Pipe
   * @param argumentFieldSelector of type Fields
   * @param filter                of type Filter
   */
  public Each( Pipe pipe, Fields argumentFieldSelector, Filter filter )
    {
    super( pipe, argumentFieldSelector, filter, FILTER_SELECTOR );
    }

  ////////////////////
  // TAKE ASSERTIONS
  ///////////////////

  /**
   * Constructor Each creates a new Each instance.
   *
   * @param name           of type String
   * @param assertionLevel of type AssertionLevel
   * @param assertion      of type Filter
   */
  public Each( String name, AssertionLevel assertionLevel, Assertion assertion )
    {
    super( name, assertionLevel, assertion, FILTER_SELECTOR );
    }

  /**
   * @param name                  of type String
   * @param argumentFieldSelector of type Fields
   * @param assertionLevel        of type AssertionLevel
   * @param assertion             of type Filter
   */
  public Each( String name, Fields argumentFieldSelector, AssertionLevel assertionLevel, Assertion assertion )
    {
    super( name, argumentFieldSelector, assertionLevel, assertion, FILTER_SELECTOR );
    }

  /**
   * @param previous       of type Pipe
   * @param assertionLevel of type AssertionLevel
   * @param assertion      of type Filter
   */
  public Each( Pipe previous, AssertionLevel assertionLevel, Assertion assertion )
    {
    super( previous, assertionLevel, assertion, FILTER_SELECTOR );
    }

  /**
   * @param previous              of type Pipe
   * @param argumentFieldSelector of type Fields
   * @param assertionLevel        of type AssertionLevel
   * @param assertion             of type Filter
   */
  public Each( Pipe previous, Fields argumentFieldSelector, AssertionLevel assertionLevel, Assertion assertion )
    {
    super( previous, argumentFieldSelector, assertionLevel, assertion, FILTER_SELECTOR );
    }

  @Override
  protected void verifyOperation()
    {
    super.verifyOperation();

    if( !argumentSelector.isArgSelector() )
      throw new IllegalArgumentException( "invalid argument selector: " + argumentSelector );

    if( !operation.getFieldDeclaration().isDeclarator() )
      throw new IllegalArgumentException( "invalid field declaration: " + operation.getFieldDeclaration() );

    if( !outputSelector.isOutSelector() )
      throw new IllegalArgumentException( "invalid output selector: " + outputSelector );
    }

  private Function getFunction()
    {
    return (Function) operation;
    }

  private Filter getFilter()
    {
    return (Filter) operation;
    }

  private ValueAssertion getValueAssertion()
    {
    return (ValueAssertion) operation;
    }

  private boolean isFunction()
    {
    return operation instanceof Function;
    }

  private boolean isFilter()
    {
    return operation instanceof Filter;
    }

  private void applyAssertion( FlowCollector flowCollector, TupleEntry input, TupleEntry arguments )
    {
    getValueAssertion().doAssert( arguments );

    flowCollector.collect( input.getTuple() );
    }

  private void applyFilter( FlowCollector flowCollector, TupleEntry input, TupleEntry arguments )
    {
    boolean isRemove = false;

    isRemove = getFilter().isRemove( arguments );

    if( !isRemove )
      flowCollector.collect( input.getTuple() );
    }

  private void applyFunction( final FlowCollector flowCollector, final TupleEntry input, TupleEntry arguments, final TupleEntry declaredEntry, final Fields outgoingSelector )
    {
    TupleCollector tupleCollector = new TupleCollector( declaredEntry.getFields() )
    {
    protected void collect( Tuple tuple )
      {
      flowCollector.collect( makeResult( outgoingSelector, input, declaredEntry, tuple ) );
      }
    };

    getFunction().operate( arguments, tupleCollector ); // adds results to collector
    }

  // FIELDS

  private Fields getFieldsFor( Scope incomingScope )
    {
    if( incomingScope.isEvery() )
      return incomingScope.getOutGroupingFields();
    else
      return incomingScope.getOutValuesFields();
    }

  @Override
  public Fields resolveIncomingOperationFields( Scope incomingScope )
    {
    return getFieldsFor( incomingScope );
    }

  @Override
  public Fields resolveFields( Scope scope )
    {
    return getFieldsFor( scope );
    }

  /** @see Operator#outgoingScopeFor(Set<Scope>) */
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    Fields argumentSelector = resolveArgumentSelector( incomingScopes );

    verifyArguments( argumentSelector );

    Fields declared = resolveDeclared( incomingScopes, argumentSelector );

    verifyDeclared( declared );

    Fields outgoingValuesSelector = resolveOutgoingValuesSelector( incomingScopes, argumentSelector, declared );

    verifyOutputSelector( outgoingValuesSelector );

    Fields outgoingGrouping = Fields.asDeclaration( outgoingValuesSelector );

    return new Scope( getName(), Scope.Kind.EACH, argumentSelector, declared, outgoingGrouping, outgoingValuesSelector );
    }

  Fields resolveOutgoingValuesSelector( Set<Scope> incomingScopes, Fields argumentSelector, Fields declared )
    {
    try
      {
      return resolveOutgoingSelector( incomingScopes, argumentSelector, declared );
      }
    catch( Exception exception )
      {
      throw new OperatorException( "could not resolve outgoing values selector in: " + this, exception );
      }
    }

  public EachHandler getHandler()
    {
    if( isFunction() )
      return new EachFunctionHandler();
    else if( isFilter() )
      return new EachFilterHandler();
    else
      return new EachAssertionHandler();
    }

  /** Class EachHandler is a helper class that wraps Each instances. */
  public abstract class EachHandler
    {
    public void operate( Scope scope, TupleEntry input, FlowCollector flowCollector )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( operation + " incoming entry: " + input );

      TupleEntry arguments = scope.getArgumentsEntry( input );

      if( LOG.isDebugEnabled() )
        LOG.debug( operation + " arg entry: " + arguments );

      handle( scope, flowCollector, input, arguments );
      }

    abstract void handle( Scope scope, FlowCollector flowCollector, TupleEntry input, TupleEntry arguments );

    public FlowElement getEach()
      {
      return Each.this;
      }
    }

  public class EachFunctionHandler extends EachHandler
    {
    void handle( Scope scope, FlowCollector flowCollector, TupleEntry input, TupleEntry arguments )
      {
      applyFunction( flowCollector, input, arguments, scope.getDeclaredEntry(), scope.getOutValuesSelector() );
      }
    }

  public class EachFilterHandler extends EachHandler
    {
    void handle( Scope scope, FlowCollector flowCollector, TupleEntry input, TupleEntry arguments )
      {
      applyFilter( flowCollector, input, arguments );
      }
    }

  public class EachAssertionHandler extends EachHandler
    {
    void handle( Scope scope, FlowCollector flowCollector, TupleEntry input, TupleEntry arguments )
      {
      applyAssertion( flowCollector, input, arguments );
      }
    }


  }
