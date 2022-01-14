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
import cascading.tuple.type.ToCanonical;
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
public class CanonicalBench
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
  Canonical to = Canonical.String;

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

  @Param({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"})
  int from = 0;

  Object[] fromValues = new Object[]{
    null,
    "1000",
    (short) 1000,
    (short) 1000,
    1000,
    1000,
    1000L,
    1000L,
    1000.000F,
    1000.000F,
    1000.000D,
    1000.000D
  };

  Class[] fromTypes = new Class[]{
    String.class,
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
  ToCanonical canonical;
  Object fromValue;
  Class fromType;

  @Setup
  public void setup()
    {
    coercibleType = Coercions.coercibleTypeFor( canonicalTypes[ to.ordinal() ] );
    fromType = fromTypes[ from ];
    canonical = coercibleType.from( fromType );
    fromValue = fromValues[ from ];
    }

  @Benchmark
  public void baseline( Blackhole bh )
    {
    bh.consume( coercibleType.canonical( fromValue ) );
    }

  @Benchmark
  public void toCanonical( Blackhole bh )
    {
    bh.consume( coercibleType.from( fromType ).canonical( fromValue ) );
    }

  @Benchmark
  public void toCanonicalFixed( Blackhole bh )
    {
    bh.consume( canonical.canonical( fromValue ) );
    }
  }
