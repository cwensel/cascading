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

package cascading.tuple;

import java.io.IOException;

/**
 * Interface TupleEntryCollector is used to allow {@link cascading.operation.BaseOperation} instances to emit
 * one or more result {@link Tuple} values.
 * <p/>
 * The general rule in Cascading is if you are handed a Tuple, you cannot change or cache it. Attempts at modifying
 * such a Tuple will result in an Exception. Preventing caching is harder, see below.
 * <p/>
 * If you create the Tuple, you can re-use or modify it.
 * <p/>
 * When calling {@link #add(Tuple)} or {@link #add(TupleEntry)}, you are passing a Tuple to the down stream pipes and
 * operations. Since no downstream operation may modify or cache the Tuple instance, it is safe to re-use the Tuple
 * instance when {@code add()} returns.
 * <p/>
 * That said, Tuple copies do get cached in order to perform specific operations in the underlying platforms. Currently
 * only a shallow copy is made (via the {@link Tuple} copy constructor). Thus, any mutable type or collection
 * placed inside a Tuple will not be copied, but will likely be cached if a copy of the Tuple passed downstream is
 * copied.
 * <p/>
 * So any subsequent changes to that nested type or collection will be reflected in the cached copy, a likely
 * source of hard to find errors.
 * <p/>
 * There is currently no way to specify that a deep copy must be performed when making a Tuple copy.
 */
public abstract class TupleEntryCollector
  {
  protected TupleEntry tupleEntry = new TupleEntry( Fields.UNKNOWN, null, true );

  protected TupleEntryCollector()
    {
    }

  /**
   * Constructor TupleCollector creates a new TupleCollector instance.
   *
   * @param declared of type Fields
   */
  public TupleEntryCollector( Fields declared )
    {
    setFields( declared );
    }

  public void setFields( Fields declared )
    {
    if( declared == null )
      throw new IllegalArgumentException( "declared fields must not be null" );

    if( declared.isUnknown() || declared.isAll() )
      return;

    this.tupleEntry = new TupleEntry( declared, Tuple.size( declared.size() ), true );
    }

  /**
   * Method add inserts the given {@link TupleEntry} into the outgoing stream. Note the method {@link #add(Tuple)} is
   * more efficient as it simply calls {@link TupleEntry#getTuple()};
   * <p/>
   * See {@link cascading.tuple.TupleEntryCollector} on when and how to re-use a Tuple instance.
   *
   * @param tupleEntry of type TupleEntry
   */
  public void add( TupleEntry tupleEntry )
    {
    Fields expectedFields = this.tupleEntry.getFields();
    TupleEntry outgoingEntry = this.tupleEntry;

    if( expectedFields.isUnknown() || expectedFields.equals( tupleEntry.getFields() ) )
      outgoingEntry = tupleEntry;
    else
      outgoingEntry.setTuple( selectTupleFrom( tupleEntry, expectedFields ) );

    safeCollect( outgoingEntry );
    }

  private Tuple selectTupleFrom( TupleEntry tupleEntry, Fields expectedFields )
    {
    try
      {
      return tupleEntry.selectTuple( expectedFields );
      }
    catch( TupleException exception )
      {
      Fields givenFields = tupleEntry.getFields();
      String string = "given TupleEntry fields: " + givenFields.printVerbose();
      string += " do not match the operation declaredFields: " + expectedFields.printVerbose();
      string += ", operations must emit tuples that match the fields they declare as output";

      throw new TupleException( string, exception );
      }
    }

  /**
   * Method add inserts the given {@link Tuple} into the outgoing stream.
   * <p/>
   * See {@link cascading.tuple.TupleEntryCollector} on when and how to re-use a Tuple instance.
   *
   * @param tuple of type Tuple
   */
  public void add( Tuple tuple )
    {
    if( !tupleEntry.getFields().isUnknown() && tupleEntry.getFields().size() != tuple.size() )
      throw new TupleException( "operation added the wrong number of fields, expected: " + tupleEntry.getFields().print() + ", got result size: " + tuple.size() );

    boolean isUnmodifiable = tuple.isUnmodifiable();

    tupleEntry.setTuple( tuple );

    try
      {
      safeCollect( tupleEntry );
      }
    finally
      {
      Tuples.setUnmodifiable( tuple, isUnmodifiable );
      }
    }

  private void safeCollect( TupleEntry tupleEntry )
    {
    try
      {
      collect( tupleEntry );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to collect tuple", exception );
      }
    }

  protected abstract void collect( TupleEntry tupleEntry ) throws IOException;

  /**
   * Method close closes the underlying resource being written to.
   * <p/>
   * This method should be called when when an instance is returned via
   * {@link cascading.tap.Tap#openForWrite(cascading.flow.FlowProcess)}
   * and no more {@link Tuple} instances will be written out.
   * <p/>
   * This method must not be called when an instance is returned from {@code getOutputCollector()} from any of
   * the relevant {@link cascading.operation.OperationCall} implementations (inside a Function, Aggregator, or Buffer).
   */
  public void close()
    {
    // do nothing
    }
  }