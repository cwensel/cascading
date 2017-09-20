/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;

/**
 * TupleStream provides helper methods to create {@link Tuple} {@link Stream} instances from {@link Tap} instances.
 * <p>
 * This class is a convenience class over the methods provided on the Tap class that provide the same
 * functionality. This class exists to help overcome any generics compiler warnings.
 * <p>
 * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
 * is simply accomplished with a try-with-resources statement.
 */
public class TupleStream
  {
  /**
   * Method tupleStream returns a {@link Stream} of {@link Tuple} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link Tap#tupleStream(FlowProcess)}.
   * <p>
   * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
   * is simply accomplished with a try-with-resources statement.
   *
   * @param tap         the Tap to open
   * @param flowProcess represents the current platform configuration
   * @return a Stream of Tuple instances
   */
  @SuppressWarnings("unchecked")
  public static Stream<Tuple> tupleStream( Tap tap, FlowProcess flowProcess )
    {
    Objects.requireNonNull( tap );

    return tap.tupleStream( flowProcess );
    }

  /**
   * Method tupleStreamCopy returns a {@link Stream} of {@link Tuple} instances from the given
   * {@link Tap} instance.
   * <p>
   * This method returns an Tuple instance suitable for caching.
   * <p>
   * Also see {@link Tap#tupleStreamCopy(FlowProcess)}.
   * <p>
   * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
   * is simply accomplished with a try-with-resources statement.
   *
   * @param tap         the Tap to open
   * @param flowProcess represents the current platform configuration
   * @return a Stream of TupleEntry instances
   */
  @SuppressWarnings("unchecked")
  public static Stream<Tuple> tupleStreamCopy( Tap tap, FlowProcess flowProcess )
    {
    Objects.requireNonNull( tap );

    return tap.tupleStreamCopy( flowProcess );
    }

  /**
   * Method tupleStream returns a {@link Stream} of {@link Tuple} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link Tap#tupleStream(FlowProcess)}.
   * <p>
   * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
   * is simply accomplished with a try-with-resources statement.
   *
   * @param tap         the Tap to open
   * @param flowProcess represents the current platform configuration
   * @param selector    the fields to select from the underlying Tuple
   * @return a Stream of TupleE instances
   */
  @SuppressWarnings("unchecked")
  public static Stream<Tuple> tupleStream( Tap tap, FlowProcess flowProcess, Fields selector )
    {
    Objects.requireNonNull( tap );
    Objects.requireNonNull( selector );

    return tap.tupleStream( flowProcess, selector );
    }

  /**
   * Method tupleStreamCopy returns a {@link Stream} of {@link Tuple} instances from the given
   * {@link Tap} instance.
   * <p>
   * This method returns an Tuple instance suitable for caching.
   * <p>
   * Also see {@link Tap#tupleStreamCopy(FlowProcess)}.
   * <p>
   * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
   * is simply accomplished with a try-with-resources statement.
   *
   * @param tap         the Tap to open
   * @param flowProcess represents the current platform configuration
   * @param selector    the fields to select from the underlying Tuple
   * @return a Stream of Tuple instances
   */
  @SuppressWarnings("unchecked")
  public static Stream<Tuple> tupleStreamCopy( Tap tap, FlowProcess flowProcess, Fields selector )
    {
    Objects.requireNonNull( tap );
    Objects.requireNonNull( selector );

    return tap.tupleStreamCopy( flowProcess, selector );
    }

  /**
   * Method posToObject returns the object at the given tuple position.
   *
   * @param pos the ordinal position to select from
   * @param <R> the object type
   * @return the value in the given ordinal position
   */
  @SuppressWarnings("unchecked")
  public static <R> Function<Tuple, ? extends R> posToObject( int pos )
    {
    return value -> (R) value.getObject( pos );
    }

  /**
   * Method posToInt returns the int value at the given tuple position.
   *
   * @param pos the ordinal position to select from
   * @return the value in the given ordinal position
   */
  public static ToIntFunction<Tuple> posToInt( int pos )
    {
    return value -> value.getInteger( pos );
    }

  /**
   * Method posToLong returns the long value at the given tuple position.
   *
   * @param pos the ordinal position to select from
   * @return the value in the given ordinal position
   */
  public static ToLongFunction<Tuple> posToLong( int pos )
    {
    return value -> value.getLong( pos );
    }

  /**
   * Method posToDouble returns the double value at the given tuple position.
   *
   * @param pos the ordinal position to select from
   * @return the value in the given ordinal position
   */
  public static ToDoubleFunction<Tuple> posToDouble( int pos )
    {
    return value -> value.getDouble( pos );
    }

  /**
   * Method writeTuple will add each {@link Tuple} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of Tuple instances
   * @param into        a Supplier that returns the Tap to sink each entry into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  public static Tap writeTuple( Stream<Tuple> stream, Supplier<Tap> into, FlowProcess flowProcess )
    {
    return writeTuple( stream, into.get(), flowProcess );
    }

  /**
   * Method writeTuple will add each {@link Tuple} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of Tuple instances
   * @param into        the Tap to sink each entry into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  @SuppressWarnings("unchecked")
  public static Tap writeTuple( Stream<Tuple> stream, Tap into, FlowProcess flowProcess )
    {
    Objects.requireNonNull( into );
    Objects.requireNonNull( stream );

    try
      {
      TupleEntryCollector collector = into.openForWrite( flowProcess );

      stream.forEach( collector::add );

      collector.close();
      }
    catch( IOException exception )
      {
      throw new UncheckedIOException( exception );
      }

    return into;
    }

  /**
   * Method writeInt will add each {@code int} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of int values
   * @param into        a Supplier that returns the Tap to sink each value into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  public static Tap writeInt( IntStream stream, Supplier<Tap> into, FlowProcess flowProcess )
    {
    return writeInt( stream, into.get(), flowProcess );
    }

  /**
   * Method writeInt will add each {@code int} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of int values
   * @param into        the Tap to sink each value into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  @SuppressWarnings("unchecked")
  public static Tap writeInt( IntStream stream, Tap into, FlowProcess flowProcess )
    {
    Objects.requireNonNull( into );
    Objects.requireNonNull( stream );

    Tuple tuple = Tuple.size( 1 );

    try
      {
      TupleEntryCollector collector = into.openForWrite( flowProcess );

      stream.forEach( i -> collector.add( reset( tuple, i ) ) );

      collector.close();
      }
    catch( IOException exception )
      {
      throw new UncheckedIOException( exception );
      }

    return into;
    }

  /**
   * Method writeLong will add each {@code long} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of long values
   * @param into        a Supplier that returns the Tap to sink each value into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  public static Tap writeLong( LongStream stream, Supplier<Tap> into, FlowProcess flowProcess )
    {
    return writeLong( stream, into.get(), flowProcess );
    }

  /**
   * Method writeLong will add each {@code long} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of long values
   * @param into        the Tap to sink each value into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  @SuppressWarnings("unchecked")
  public static Tap writeLong( LongStream stream, Tap into, FlowProcess flowProcess )
    {
    Objects.requireNonNull( into );
    Objects.requireNonNull( stream );

    Tuple tuple = Tuple.size( 1 );

    try
      {
      TupleEntryCollector collector = into.openForWrite( flowProcess );

      stream.forEach( i -> collector.add( reset( tuple, i ) ) );

      collector.close();
      }
    catch( IOException exception )
      {
      throw new UncheckedIOException( exception );
      }

    return into;
    }

  /**
   * Method writeDouble will add each {@code double} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of double values
   * @param into        a Supplier that returns the Tap to sink each value into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  public static Tap writeDouble( DoubleStream stream, Supplier<Tap> into, FlowProcess flowProcess )
    {
    return writeDouble( stream, into.get(), flowProcess );
    }

  /**
   * Method writeDouble will add each {@code double} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of double values
   * @param into        the Tap to sink each value into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  @SuppressWarnings("unchecked")
  public static Tap writeDouble( DoubleStream stream, Tap into, FlowProcess flowProcess )
    {
    Objects.requireNonNull( into );
    Objects.requireNonNull( stream );

    Tuple tuple = Tuple.size( 1 );

    try
      {
      TupleEntryCollector collector = into.openForWrite( flowProcess );

      stream.forEach( i -> collector.add( reset( tuple, i ) ) );

      collector.close();
      }
    catch( IOException exception )
      {
      throw new UncheckedIOException( exception );
      }

    return into;
    }

  private static Tuple reset( Tuple tuple, Object value )
    {
    tuple.set( 0, value );
    return tuple;
    }
  }
