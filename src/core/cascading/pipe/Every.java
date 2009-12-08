/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
import java.util.Iterator;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowCollector;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.operation.Aggregator;
import cascading.operation.AssertionLevel;
import cascading.operation.Buffer;
import cascading.operation.ConcreteCall;
import cascading.operation.GroupAssertion;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.Tuples;

/**
 * The Every operator applies an {@link Aggregator} or {@link Buffer} to every grouping.
 * <p/>
 * Any number of Every instances may follow other Every, {@link GroupBy}, {@link CoGroup} instance if they apply an Aggregator, not a Buffer.
 * If a Buffer, only one Every may follow a GroupBy or CoGroup.
 * <p/>
 * Every operators create aggregate values for every grouping they encounter. This aggregate value is added to the current
 * grouping Tuple. Subsequent Every instances can continue to append values to the grouping Tuple. When an Each follows
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

  private Aggregator getAggregator()
    {
    return (Aggregator) operation;
    }

  private Buffer getReducer()
    {
    return (Buffer) operation;
    }

  private GroupAssertion getGroupAssertion()
    {
    return (GroupAssertion) operation;
    }

  @Override
  public Fields resolveIncomingOperationFields( Scope incomingScope )
    {
    if( incomingScope.isEach() || incomingScope.isTap() )
      throw new IllegalStateException( "Every cannot follow a Tap or an Each" );

    return incomingScope.getOutValuesFields();
    }

  @Override
  public Fields resolveFields( Scope scope )
    {
    if( scope.isEach() || scope.isTap() )
      throw new IllegalStateException( "Every cannot follow a Tap or an Each" );

    if( isBuffer() )
      return scope.getOutValuesFields();
    else
      return scope.getOutGroupingFields();
    }

  /** @see Operator#outgoingScopeFor(Set<Scope>) */
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    Fields argumentFields = resolveArgumentSelector( incomingScopes );

    verifyArguments( argumentFields );

    // we currently don't support using result from a previous Every in the current Every
    Scope scope = getFirst( incomingScopes );

    if( scope.isEvery() && argumentFields.contains( scope.getDeclaredFields() ) )
      throw new OperatorException( this, "arguments may not select a declared field from a previous Every" );

    Fields declaredFields = resolveDeclared( incomingScopes, argumentFields );

    verifyDeclaredFields( declaredFields );

    Fields outgoingGroupingFields = resolveOutgoingGroupingSelector( incomingScopes, argumentFields, declaredFields );

    verifyOutputSelector( outgoingGroupingFields );

    Fields outgoingValuesFields = resolveOutgoingValues( incomingScopes );

    Fields remainderFields = resolveRemainderFields( incomingScopes, argumentFields );

    return new Scope( getName(), Scope.Kind.EVERY, remainderFields, argumentFields, declaredFields, outgoingGroupingFields, outgoingValuesFields );
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

  Fields resolveOutgoingValues( Set<Scope> incomingScopes )
    {
    // Every never modifies the value stream, just the grouping stream
    try
      {
      return getFirst( incomingScopes ).getOutValuesFields();
      }
    catch( Exception exception )
      {
      throw new OperatorException( this, "could not resolve outgoing values selector in: " + this, exception );
      }
    }

  /**
   * Method getHandler returns the {@link EveryHandler} for this instnce.
   *
   * @param outgoingScope of type Scope
   * @return EveryHandler
   */
  public EveryHandler getHandler( Scope outgoingScope )
    {
    if( isAssertion() )
      return new EveryAssertionHandler( outgoingScope );
    else if( isAggregator() )
      return new EveryAggregatorHandler( outgoingScope );
    else
      return new EveryBufferHandler( outgoingScope );
    }

  /** Class EveryHandler is a helper class that wraps Every instances. */
  public abstract class EveryHandler
    {
    /** Field outgoingScope */
    public final Scope outgoingScope;
    /** Field outputCollector */
    public FlowCollector outputCollector;
    /** Field operationCall */
    ConcreteCall operationCall;

    public EveryHandler( Scope outgoingScope )
      {
      this.outgoingScope = outgoingScope;
      this.operationCall = new ConcreteCall();
      }

    public abstract void start( FlowProcess flowProcess, TupleEntry groupEntry );

    public abstract void operate( FlowProcess flowProcess, TupleEntry groupEntry, TupleEntry inputEntry, TupleEntryIterator tupleEntryIterator );

    public abstract void complete( FlowProcess flowProcess, TupleEntry groupEntry );


    @Override
    public String toString()
      {
      return Every.this.toString();
      }

    public Every getEvery()
      {
      return Every.this;
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

  public class EveryAggregatorHandler extends EveryHandler
    {
    EveryTupleCollector tupleCollector;

    private abstract class EveryTupleCollector extends TupleEntryCollector
      {
      TupleEntry value;

      public EveryTupleCollector( Fields fields )
        {
        super( fields );
        }
      }

    public EveryAggregatorHandler( final Scope outgoingScope )
      {
      super( outgoingScope );

      tupleCollector = new EveryTupleCollector( outgoingScope.getDeclaredFields() )
      {
      protected void collect( Tuple tuple )
        {
        outputCollector.collect( makeResult( outgoingScope.getOutGroupingSelector(), value, outgoingScope.getRemainderFields(), outgoingScope.getDeclaredEntry(), tuple ) );
        }
      };
      }

    public void start( FlowProcess flowProcess, TupleEntry groupEntry )
      {
      operationCall.setArguments( null );  // zero it out
      operationCall.setOutputCollector( null ); // zero it out
      operationCall.setGroup( groupEntry );

      try
        {
        getAggregator().start( flowProcess, operationCall );
        }
      catch( CascadingException exception )
        {
        throw exception;
        }
      catch( Exception exception )
        {
        throw new OperatorException( Every.this, "operator Every failed starting aggregator", exception );
        }
      }

    public void operate( FlowProcess flowProcess, TupleEntry groupEntry, TupleEntry inputEntry, TupleEntryIterator tupleEntryIterator )
      {
      try
        {
        TupleEntry arguments = outgoingScope.getArgumentsEntry( inputEntry );

        operationCall.setArguments( arguments );

        getAggregator().aggregate( flowProcess, operationCall );
        }
      catch( CascadingException exception )
        {
        throw exception;
        }
      catch( Throwable throwable )
        {
        throw new OperatorException( Every.this, "operator Every failed executing aggregator: " + operation, throwable );
        }
      }

    public void complete( FlowProcess flowProcess, TupleEntry groupEntry )
      {
      tupleCollector.value = groupEntry;

      operationCall.setArguments( null );
      operationCall.setOutputCollector( tupleCollector );

      try
        {
        getAggregator().complete( flowProcess, operationCall );
        }
      catch( CascadingException exception )
        {
        throw exception;
        }
      catch( Exception exception )
        {
        throw new OperatorException( Every.this, "operator Every failed completing aggregator", exception );
        }
      }
    }

  public class EveryBufferHandler extends EveryHandler
    {
    EveryTupleCollector tupleCollector;

    private abstract class EveryTupleCollector extends TupleEntryCollector
      {
      TupleEntry value;

      public EveryTupleCollector( Fields fields )
        {
        super( fields );
        }
      }

    public EveryBufferHandler( final Scope outgoingScope )
      {
      super( outgoingScope );

      tupleCollector = new EveryTupleCollector( outgoingScope.getDeclaredFields() )
      {
      protected void collect( Tuple tuple )
        {
        outputCollector.collect( makeResult( outgoingScope.getOutGroupingSelector(), value, outgoingScope.getRemainderFields(), outgoingScope.getDeclaredEntry(), tuple ) );
        }
      };
      }

    public TupleEntry getLastValue()
      {
      return tupleCollector.value;
      }

    public void start( FlowProcess flowProcess, TupleEntry groupEntry )
      {
      }

    public void operate( FlowProcess flowProcess, TupleEntry groupEntry, TupleEntry inputEntry, final TupleEntryIterator tupleEntryIterator )
      {
      // we want to null out any 'values' before and after the iterator begins/ends
      // this allows buffers to emit tuples before next() and when hasNext() return false;
      final TupleEntry tupleEntry = tupleEntryIterator.getTupleEntry();
      final Tuple valueNulledTuple = Tuples.setOnEmpty( tupleEntry, groupEntry );
      tupleEntry.setTuple( valueNulledTuple );

      tupleCollector.value = tupleEntry; // null out header entries

      operationCall.setOutputCollector( tupleCollector );
      operationCall.setGroup( groupEntry );

      operationCall.setArgumentsIterator( new Iterator<TupleEntry>()
      {
      public boolean hasNext()
        {
        boolean hasNext = tupleEntryIterator.hasNext();

        if( !hasNext )
          tupleEntry.setTuple( valueNulledTuple ); // null out footer entries

        return hasNext;
        }

      public TupleEntry next()
        {
        return outgoingScope.getArgumentsEntry( (TupleEntry) tupleEntryIterator.next() );
        }

      public void remove()
        {
        tupleEntryIterator.remove();
        }
      } );

      try
        {
        getReducer().operate( flowProcess, operationCall );
        }
      catch( CascadingException exception )
        {
        throw exception;
        }
      catch( Throwable throwable )
        {
        throw new OperatorException( Every.this, "operator Every failed executing buffer: " + operation, throwable );
        }
      }

    public void complete( FlowProcess flowProcess, TupleEntry groupEntry )
      {
      }
    }

  public class EveryAssertionHandler extends EveryHandler
    {
    public EveryAssertionHandler( Scope outgoingScope )
      {
      super( outgoingScope );
      }

    public void start( FlowProcess flowProcess, TupleEntry groupEntry )
      {
      operationCall.setArguments( null );
      operationCall.setOutputCollector( null ); // zero it out
      operationCall.setGroup( groupEntry );

      getGroupAssertion().start( flowProcess, operationCall );
      }

    public void operate( FlowProcess flowProcess, TupleEntry groupEntry, TupleEntry inputEntry, TupleEntryIterator tupleEntryIterator )
      {
      TupleEntry arguments = outgoingScope.getArgumentsEntry( inputEntry );

      operationCall.setArguments( arguments );

      getGroupAssertion().aggregate( flowProcess, operationCall ); // don't catch exceptions
      }

    public void complete( FlowProcess flowProcess, TupleEntry groupEntry )
      {
      operationCall.setArguments( null );
      getGroupAssertion().doAssert( flowProcess, operationCall );

      outputCollector.collect( groupEntry.getTuple() );
      }
    }
  }
