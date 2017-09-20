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
import java.util.stream.Stream;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.type.CoercibleType;

/**
 * TupleEntryStream provides helper methods to create {@link TupleEntry} {@link Stream} instances from
 * {@link Tap} instances.
 * <p>
 * This class is a convenience class over the methods provided on the Tap class that provide the same
 * functionality. This class exists to help overcome any generics compiler warnings.
 * <p>
 * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
 * is simply accomplished with a try-with-resources statement.
 */
public class TupleEntryStream
  {
  /**
   * Method entryStream returns a {@link Stream} of {@link TupleEntry} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link Tap#entryStream(FlowProcess)}.
   * <p>
   * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
   * is simply accomplished with a try-with-resources statement.
   *
   * @param tap         the Tap to open
   * @param flowProcess represents the current platform configuration
   * @return a Stream of TupleEntry instances
   */
  @SuppressWarnings("unchecked")
  public static Stream<TupleEntry> entryStream( Tap tap, FlowProcess flowProcess )
    {
    Objects.requireNonNull( tap );

    return tap.entryStream( flowProcess );
    }

  /**
   * Method entryStreamCopy returns a {@link Stream} of {@link TupleEntry} instances from the given
   * {@link Tap} instance.
   * <p>
   * This method returns an TupleEntry instance suitable for caching.
   * <p>
   * Also see {@link Tap#entryStreamCopy(FlowProcess)}.
   * <p>
   * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
   * is simply accomplished with a try-with-resources statement.
   *
   * @param tap         the Tap to open
   * @param flowProcess represents the current platform configuration
   * @return a Stream of TupleEntry instances
   */
  @SuppressWarnings("unchecked")
  public static Stream<TupleEntry> entryStreamCopy( Tap tap, FlowProcess flowProcess )
    {
    Objects.requireNonNull( tap );

    return tap.entryStreamCopy( flowProcess );
    }

  /**
   * Method entryStream returns a {@link Stream} of {@link TupleEntry} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link Tap#entryStream(FlowProcess, Fields)}.
   * <p>
   * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
   * is simply accomplished with a try-with-resources statement.
   *
   * @param tap         the Tap to open
   * @param flowProcess represents the current platform configuration
   * @param selector    the fields to select from the underlying TupleEntry
   * @return a Stream of TupleEntry instances
   */
  @SuppressWarnings("unchecked")
  public static Stream<TupleEntry> entryStream( Tap tap, FlowProcess flowProcess, Fields selector )
    {
    Objects.requireNonNull( tap );
    Objects.requireNonNull( selector );

    return tap.entryStream( flowProcess, selector );
    }

  /**
   * Method entryStreamCopy returns a {@link Stream} of {@link TupleEntry} instances from the given
   * {@link Tap} instance.
   * <p>
   * This method returns an TupleEntry instance suitable for caching.
   * <p>
   * Also see {@link Tap#entryStreamCopy(FlowProcess)}.
   * <p>
   * Note, the returned Stream instance must be closed in order to clean up underlying resources. This
   * is simply accomplished with a try-with-resources statement.
   *
   * @param tap         the Tap to open
   * @param flowProcess represents the current platform configuration
   * @param selector    the fields to select from the underlying TupleEntry
   * @return a Stream of TupleEntry instances
   */
  @SuppressWarnings("unchecked")
  public static Stream<TupleEntry> entryStreamCopy( Tap tap, FlowProcess flowProcess, Fields selector )
    {
    Objects.requireNonNull( tap );
    Objects.requireNonNull( selector );

    return tap.entryStreamCopy( flowProcess, selector );
    }

  /**
   * Method fieldToObject returns a {@link Function} that returns the object in the given named field or position.
   *
   * @param fields the field to select, only first field will be honored
   * @param <R>    the type of the object returned
   * @return a Function returning the object or null in the selected field
   */
  @SuppressWarnings("unchecked")
  public static <R> Function<TupleEntry, ? extends R> fieldToObject( Fields fields )
    {
    Objects.requireNonNull( fields );

    return value -> (R) value.getObject( fields );
    }

  /**
   * Method fieldToObject returns a {@link Function} that returns the object in the given named field or position
   * coerced to the requested {@link CoercibleType} type.
   *
   * @param fields the field to select, only first field will be honored
   * @param <R>    the type of the object returned
   * @param type   the CoercibleType to coerce the selected value into
   * @return a Function returning the object or null in the selected field
   */
  @SuppressWarnings("unchecked")
  public static <R> Function<TupleEntry, R> fieldToObject( Fields fields, CoercibleType<R> type )
    {
    Objects.requireNonNull( fields );
    Objects.requireNonNull( type );

    return value -> (R) value.getObject( fields, type );
    }

  /**
   * Method fieldToObject returns a {@link Function} that returns the object in the given named field or position
   * coerced to the requested {@link Class}.
   *
   * @param fields the field to select, only first field will be honored
   * @param <R>    the type of the object returned
   * @param type   the Class to coerce the selected value into
   * @return a Function returning the object or null in the selected field
   */
  @SuppressWarnings("unchecked")
  public static <R> Function<TupleEntry, R> fieldToObject( Fields fields, Class<R> type )
    {
    Objects.requireNonNull( fields );
    Objects.requireNonNull( type );

    return value -> (R) value.getObject( fields, type );
    }

  /**
   * Method fieldToInt returns a {@link Function} that returns the int in the given named field or position.
   *
   * @param fields the field to select, only first field will be honored
   * @return the int value in the selected field
   */
  public static ToIntFunction<TupleEntry> fieldToInt( Fields fields )
    {
    Objects.requireNonNull( fields );

    return value -> value.getInteger( fields );
    }

  /**
   * Method fieldToInt returns a {@link Function} that returns the int in the given named field or position.
   *
   * @param name the field to select
   * @return the int value in the selected field
   */
  public static ToIntFunction<TupleEntry> fieldToInt( Comparable name )
    {
    Objects.requireNonNull( name );

    return value -> value.getInteger( name );
    }

  /**
   * Method fieldToLong returns a {@link Function} that returns the long in the given named field or position.
   *
   * @param fields the field to select, only first field will be honored
   * @return the long value in the selected field
   */
  public static ToLongFunction<TupleEntry> fieldToLong( Fields fields )
    {
    Objects.requireNonNull( fields );

    return value -> value.getLong( fields );
    }

  /**
   * Method fieldToLong returns a {@link Function} that returns the long in the given named field or position.
   *
   * @param name the field to select
   * @return the long value in the selected field
   */
  public static ToLongFunction<TupleEntry> fieldToLong( Comparable name )
    {
    Objects.requireNonNull( name );

    return value -> value.getLong( name );
    }

  /**
   * Method fieldToDouble returns a {@link Function} that returns the double in the given named field or position.
   *
   * @param fields the field to select, only first field will be honored
   * @return the double value in the selected field
   */
  public static ToDoubleFunction<TupleEntry> fieldToDouble( Fields fields )
    {
    Objects.requireNonNull( fields );

    return value -> value.getDouble( fields );
    }

  /**
   * Method fieldToDouble returns a {@link Function} that returns the double in the given named field or position.
   *
   * @param name the field to select
   * @return the double value in the selected field
   */
  public static ToDoubleFunction<TupleEntry> fieldToDouble( Comparable name )
    {
    Objects.requireNonNull( name );

    return value -> value.getDouble( name );
    }

  /**
   * Method writeEntry will add each {@link TupleEntry} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of TupleEntry instances
   * @param into        a Supplier that returns the Tap to sink each entry into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  public static Tap writeEntry( Stream<TupleEntry> stream, Supplier<Tap> into, FlowProcess flowProcess )
    {
    return writeEntry( stream, into.get(), flowProcess );
    }

  /**
   * Method writeEntry will add each {@link TupleEntry} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param stream      a Stream of TupleEntry instances
   * @param into        the Tap to sink each entry into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  @SuppressWarnings("unchecked")
  public static Tap writeEntry( Stream<TupleEntry> stream, Tap into, FlowProcess flowProcess )
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
   * Method writeEntry will add each {@link TupleEntry} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param iterable    an Iterable of TupleEntry instances
   * @param into        a Supplier that returns the Tap to sink each entry into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  public static Tap writeEntry( Iterable<TupleEntry> iterable, Supplier<Tap> into, FlowProcess flowProcess )
    {
    return writeEntry( iterable, into.get(), flowProcess );
    }

  /**
   * Method writeEntry will add each {@link TupleEntry} instance to the {@link TupleEntryCollector} provided
   * by the given {@link Tap} instance.
   *
   * @param iterable    an Iterable of TupleEntry instances
   * @param into        the Tap to sink each entry into
   * @param flowProcess represents the current platform configuration
   * @return returns the given Tap
   */
  @SuppressWarnings("unchecked")
  public static Tap writeEntry( Iterable<TupleEntry> iterable, Tap into, FlowProcess flowProcess )
    {
    Objects.requireNonNull( into );
    Objects.requireNonNull( iterable );

    try
      {
      TupleEntryCollector collector = into.openForWrite( flowProcess );

      iterable.forEach( collector::add );

      collector.close();
      }
    catch( IOException exception )
      {
      throw new UncheckedIOException( exception );
      }

    return into;
    }
  }
