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

import java.beans.ConstructorProperties;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowCollector;
import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.operation.Assertion;
import cascading.operation.AssertionLevel;
import cascading.operation.ConcreteCall;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.ValueAssertion;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
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
   * @param name     name for this branch of Pipes
   * @param function Function to be applied to each input Tuple
   */
  @ConstructorProperties({"name", "function"})
  public Each( String name, Function function )
    {
    super( name, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass arguementFields to the given function, only return fields declared by the function.
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
   * Only pass arguementFields to the given function, only return fields selected by the outFieldsSelector.
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
   * Only return fields selected by the outFieldsSelector.
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
   * Only pass arguementFields to the given function, only return fields declared by the function.
   *
   * @param previous         previous Pipe to receive input Tuples from
   * @param argumentSelector field selector that selects Function arguments from the input Tuple
   * @param function         Function to be applied to each input Tuple
   */
  @ConstructorProperties({"previoud", "argumentSelector", "function"})
  public Each( Pipe previous, Fields argumentSelector, Function function )
    {
    super( previous, argumentSelector, function, FUNCTION_SELECTOR );
    }

  /**
   * Only pass arguementFields to the given function, only return fields selected by the outFieldsSelector.
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
   * Only pass arguementFields to the given function, only return fields selected by the outFieldsSelector.
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
   * @param previous previous Pipe to receive input Tuples from
   * @param filter   Filter to be applied to each input Tuple
   */
  @ConstructorProperties({"previous", "filter"})
  public Each( Pipe previous, Filter filter )
    {
    super( previous, filter, FILTER_SELECTOR );
    }

  /**
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
  @ConstructorProperties({"previous", "debuglevel", "debug"})
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

  private void applyAssertion( FlowProcess flowProcess, FlowCollector flowCollector, TupleEntry input, ConcreteCall operationCall )
    {
    getValueAssertion().doAssert( flowProcess, operationCall );

    flowCollector.collect( input.getTuple() );
    }

  private void applyFilter( FlowProcess flowProcess, FlowCollector flowCollector, TupleEntry input, FilterCall filterCall )
    {
    boolean isRemove = false;

    isRemove = getFilter().isRemove( flowProcess, filterCall );

    if( !isRemove )
      flowCollector.collect( input.getTuple() );
    }

  private void applyFunction( FlowProcess flowProcess, FunctionCall functionCall )
    {
    getFunction().operate( flowProcess, functionCall ); // adds results to collector
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
    Fields argumentFields = resolveArgumentSelector( incomingScopes );

    verifyArguments( argumentFields );

    Fields declaredFields = resolveDeclared( incomingScopes, argumentFields );

    verifyDeclaredFields( declaredFields );

    Fields outgoingValuesFields = resolveOutgoingValuesSelector( incomingScopes, argumentFields, declaredFields );

    verifyOutputSelector( outgoingValuesFields );

    Fields outgoingGroupingFields = Fields.asDeclaration( outgoingValuesFields );

    Fields remainderFields = resolveRemainderFields( incomingScopes, argumentFields );

    return new Scope( getName(), Scope.Kind.EACH, remainderFields, argumentFields, declaredFields, outgoingGroupingFields, outgoingValuesFields );
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

  public EachHandler getHandler( FlowCollector flowCollector, Scope scope )
    {
    if( isFunction() )
      return new EachFunctionHandler( flowCollector, scope );
    else if( isFilter() )
      return new EachFilterHandler( flowCollector, scope );
    else
      return new EachAssertionHandler( flowCollector, scope );
    }

  /** Class EachHandler is a helper class that wraps Each instances. */
  public abstract class EachHandler
    {
    FlowCollector flowCollector;
    final Scope scope;
    protected ConcreteCall operationCall;

    protected EachHandler( FlowCollector flowCollector, Scope scope )
      {
      this.flowCollector = flowCollector;
      this.scope = scope;
      operationCall = new ConcreteCall();
      }

    public void operate( FlowProcess flowProcess, TupleEntry input )
      {
      try
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( operation + " incoming entry: " + input );

        TupleEntry arguments = scope.getArgumentsEntry( input );

        if( LOG.isDebugEnabled() )
          LOG.debug( operation + " arg entry: " + arguments );

        handle( flowProcess, input, arguments );
        }
      catch( CascadingException exception )
        {
        throw exception;
        }
      catch( Throwable exception )
        {
        throw new OperatorException( Each.this, "operator Each failed executing operation", exception );
        }
      }

    abstract void handle( FlowProcess flowProcess, TupleEntry input, TupleEntry arguments );

    public FlowElement getEach()
      {
      return Each.this;
      }

    public void prepare( FlowProcess flowProcess )
      {
      getOperation().prepare( flowProcess, operationCall );
      }

    public void cleanup( FlowProcess flowProcess )
      {
      getOperation().cleanup( flowProcess, operationCall );
      }
    }

  public class EachFunctionHandler extends EachHandler
    {
    EachTupleCollector tupleCollector;

    private abstract class EachTupleCollector extends TupleEntryCollector
      {
      Scope scope;
      TupleEntry input;

      private EachTupleCollector( Fields fields, Scope scope )
        {
        super( fields );
        this.scope = scope;
        }
      }

    public EachFunctionHandler( final FlowCollector flowCollector, Scope scope )
      {
      super( flowCollector, scope );

      tupleCollector = new EachTupleCollector( scope.getDeclaredEntry().getFields(), scope )
      {
      protected void collect( Tuple tuple )
        {
        flowCollector.collect( makeResult( scope.getOutValuesSelector(), input, scope.getRemainderFields(), scope.getDeclaredEntry(), tuple ) );
        }
      };

      operationCall.setOutputCollector( tupleCollector );
      }

    void handle( FlowProcess flowProcess, TupleEntry input, TupleEntry arguments )
      {
      tupleCollector.input = input;
      operationCall.setArguments( arguments );
      applyFunction( flowProcess, operationCall );
      }
    }

  public class EachFilterHandler extends EachHandler
    {

    public EachFilterHandler( FlowCollector flowCollector, Scope scope )
      {
      super( flowCollector, scope );
      }

    void handle( FlowProcess flowProcess, TupleEntry input, TupleEntry arguments )
      {
      operationCall.setArguments( arguments );
      applyFilter( flowProcess, flowCollector, input, operationCall );
      }
    }

  public class EachAssertionHandler extends EachHandler
    {
    public EachAssertionHandler( FlowCollector flowCollector, Scope scope )
      {
      super( flowCollector, scope );
      }

    void handle( FlowProcess flowProcess, TupleEntry input, TupleEntry arguments )
      {
      operationCall.setArguments( arguments );
      applyAssertion( flowProcess, flowCollector, input, operationCall );
      }
    }
  }
