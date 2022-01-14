/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
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

package cascading.tuple;

import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.CoercionFrom;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 *
 */
@State(Scope.Thread)
@Warmup(iterations = 1, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 250, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CoerceBench
  {
  public enum Canonical
    {
      String,
      Short,
      Short_TYPE,
      Integer,
      Integer_TYPE,
      Long,
      Long_TYPE,
      Float,
      Float_TYPE,
      Double,
      Double_TYPE
    }

  @Param
  Canonical from = Canonical.String;

  Type[] canonicalTypes = new Type[]{
    String.class,
    Short.class,
    Short.TYPE,
    Integer.class,
    Integer.TYPE,
    Long.class,
    Long.TYPE,
    Float.class,
    Float.TYPE,
    Double.class,
    Double.TYPE
  };

  Object[] canonicalValues = new Object[]{
    "1000",
    1000,
    1000,
    1000,
    1000,
    1000L,
    1000L,
    1000.000F,
    1000.000F,
    1000.000D,
    1000.000D
  };

  @Param({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"})
  int to = 0;

  Class[] toTypes = new Class[]{
    String.class,
    Short.class,
    Short.TYPE,
    Integer.class,
    Integer.TYPE,
    Long.class,
    Long.TYPE,
    Float.class,
    Float.TYPE,
    Double.class,
    Double.TYPE
  };

  CoercibleType coercibleType;
  Object canonicalValue;
  CoercionFrom coercion;
  Class toType;

  @Setup
  public void setup()
    {
    coercibleType = Coercions.coercibleTypeFor( canonicalTypes[ from.ordinal() ] );
    canonicalValue = canonicalValues[ from.ordinal() ];
    toType = toTypes[ to ];
    coercion = coercibleType.to( toType );
    }

  @Benchmark
  public void baseline( Blackhole bh )
    {
    bh.consume( coercibleType.coerce( canonicalValue, toType ) );
    }

  @Benchmark
  public void coercionFrom( Blackhole bh )
    {
    bh.consume( coercibleType.to( toType ).coerce( canonicalValue ) );
    }

  @Benchmark
  public void coercionFromFixed( Blackhole bh )
    {
    bh.consume( coercion.coerce( canonicalValue ) );
    }
  }