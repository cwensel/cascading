/*
 * Copyright (c) 2007-2023 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tuple.type;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.function.Supplier;

import cascading.CascadingException;
import cascading.operation.SerFunction;
import cascading.util.Util;

/**
 * Class InstantType is an implementation of {@link CoercibleType}.
 * <p>
 * This CoercibleType allows for an {@link Instant} to be maintained as the canonical type in a Tuple stream.
 * <p>
 * The Instant class maintains precision up to nanoseconds and is helpful in maintaining precision when working with
 * formats that do not support variable precisions.
 * <p>
 * Any coercion into an Instant will assume the incoming value is UTC.
 * <p>
 * Provide a {@link DateTimeFormatter} to control the String parsing and representation of the Instant.
 */
public final class InstantType implements CoercibleType<Instant>
  {
  /**
   * Second precision InstantType using the ISO instant format.
   */
  public static final InstantType ISO_SECONDS = new InstantType( ChronoUnit.SECONDS, DateTimeFormatter.ISO_INSTANT );

  /**
   * Millisecond precision InstantType using the ISO instant format.
   */
  public static final InstantType ISO_MILLIS = new InstantType( ChronoUnit.MILLIS, DateTimeFormatter.ISO_INSTANT );

  /**
   * Microsecond precision InstantType using the ISO instant format.
   */
  public static final InstantType ISO_MICROS = new InstantType( ChronoUnit.MICROS, DateTimeFormatter.ISO_INSTANT );

  /**
   * Nanosecond precision InstantType using the ISO instant format.
   */
  public static final InstantType ISO_NANOS = new InstantType( ChronoUnit.NANOS, DateTimeFormatter.ISO_INSTANT );

  private static final Long ZERO = 0L;
  private final ChronoUnit unit;
  private final Supplier<DateTimeFormatter> dateTimeFormatter;

  // transforms
  private SerFunction<Long, Instant> longCanonical;
  private SerFunction<BigDecimal, Instant> bigCanonical;
  private SerFunction<Instant, Long> longCoerce;

  private InstantType()
    {
    this( ChronoUnit.MILLIS, DateTimeFormatter.ISO_INSTANT );
    }

  public InstantType( TemporalUnit unit, DateTimeFormatter dateTimeFormatter )
    {
    this( unit, () -> dateTimeFormatter );
    }

  public InstantType( TemporalUnit unit, Supplier<DateTimeFormatter> dateTimeFormatter )
    {
    this.unit = !( unit instanceof ChronoUnit ) ? ChronoUnit.MILLIS : (ChronoUnit) unit;
    this.dateTimeFormatter = dateTimeFormatter;

    initTransforms();
    }

  protected void initTransforms()
    {
    switch( unit )
      {
      case SECONDS:
        longCanonical = Instant::ofEpochSecond;
        bigCanonical = b -> Instant.ofEpochSecond( b.longValue(), b.remainder( BigDecimal.ONE ).setScale( 9, RoundingMode.HALF_UP ).unscaledValue().longValue() );
        longCoerce = Instant::getEpochSecond;
        return;
      case MILLIS:
        longCanonical = Instant::ofEpochMilli;
        bigCanonical = b ->
          {
          BigDecimal value = b.movePointLeft( 3 );
          return Instant.ofEpochSecond( value.longValue(), value.remainder( BigDecimal.ONE ).setScale( 9, RoundingMode.HALF_UP ).unscaledValue().longValue() );
          };
        longCoerce = Instant::toEpochMilli;
        return;
      case MICROS:
        longCanonical = l -> Instant.ofEpochSecond( Math.floorDiv( l, 1_000_000L ), Math.floorMod( l, 1_000_000L ) * 1_000L );
        bigCanonical = b ->
          {
          BigDecimal value = b.movePointLeft( 6 );
          return Instant.ofEpochSecond( value.longValue(), value.remainder( BigDecimal.ONE ).setScale( 9, RoundingMode.HALF_UP ).unscaledValue().longValue() );
          };
        longCoerce = i -> Math.addExact( Math.multiplyExact( i.getEpochSecond(), 1_000_000L ), i.getLong( ChronoField.MICRO_OF_SECOND ) );
        return;
      case NANOS:
        longCanonical = l -> Instant.ofEpochSecond( Math.floorDiv( l, 1_000_000_000L ), Math.floorMod( l, 1_000_000_000L ) );
        bigCanonical = b -> Instant.ofEpochSecond( 0, b.longValue() );
        longCoerce = i -> Math.addExact( Math.multiplyExact( i.getEpochSecond(), 1_000_000_000L ), i.getLong( ChronoField.NANO_OF_SECOND ) );
        return;
      default:
        throw new IllegalArgumentException( "unsupported chrono unit: " + unit );
      }
    }

  @Override
  public Class<Instant> getCanonicalType()
    {
    return Instant.class;
    }

  @Override
  public Instant canonical( Object value )
    {
    if( value == null )
      return null;

    Class from = value.getClass();

    if( from == Instant.class )
      return (Instant) value;

    if( from == String.class )
      return getDateTimeFormatter().parse( (String) value, Instant::from );

    if( from == Date.class )
      return Instant.ofEpochMilli( ( (Date) value ).getTime() ); // in UTC

    // need to be forgiving up the upstream parser
    if( from == Integer.class )
      return longCanonical.apply( ( (Integer) value ).longValue() );

    if( from == Long.class )
      return longCanonical.apply( (Long) value );

    // this is the safest but not most absurd way to make the conversion
    if( from == Double.class )
      return bigCanonical.apply( BigDecimal.valueOf( (Double) value ) );

    if( from == BigDecimal.class )
      return bigCanonical.apply( (BigDecimal) value );

    throw new CascadingException( "unknown type coercion requested from: " + Util.getTypeName( from ) );
    }

  @Override
  public <Coerce> Coerce coerce( Object value, Type to )
    {
    if( value == null )
      return to == long.class ? (Coerce) ZERO : null;

    Class<?> from = value.getClass();

    if( from != Instant.class )
      throw new IllegalStateException( "was not normalized" );

    if( to == Instant.class || to.getClass() == InstantType.class )
      return (Coerce) value;

    if( to == Long.class || to == long.class )
      return (Coerce) longCoerce.apply( (Instant) value );

    if( to == String.class )
      return (Coerce) getDateTimeFormatter().format( (Instant) value );

    throw new CascadingException( "unknown type coercion requested, from: " + Util.getTypeName( from ) + " to: " + Util.getTypeName( to ) );
    }

  public DateTimeFormatter getDateTimeFormatter()
    {
    return dateTimeFormatter.get();
    }

  public TemporalUnit getUnit()
    {
    return unit;
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName();
    }
  }
