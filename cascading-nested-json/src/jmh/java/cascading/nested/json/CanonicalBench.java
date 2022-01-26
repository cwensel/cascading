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

package cascading.nested.json;

import java.util.concurrent.TimeUnit;

import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.ToCanonical;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
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
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CanonicalBench
  {
  @Param({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
          "11", "12", "13", "14", "15", "16", "17"})
  int from = 0;

  Object[] fromValues = new Object[]{
    JsonNodeFactory.instance.textNode( "1000" ),
    JsonNodeFactory.instance.numberNode( 1000 ),
    JSONCoercibleType.TYPE.canonical( "{ \"name\":\"John\", \"age\":50, \"car\":null }" ),
    null,
    "1000",
    "{ \"sale\":true }",
    "{\n\"person\":{ \"name\":\"John\", \"age\":50, \"city\":\"Houston\" }\n}",
    "[ \"Ford\", \"BMW\", \"Fiat\" ]",
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
    JsonNode.class,
    JsonNode.class,
    JsonNode.class,
    String.class,
    String.class,
    String.class,
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

  CoercibleType coercibleType = JSONCoercibleType.TYPE;
  ToCanonical canonical;
  Object fromValue;
  Class fromType;

  @Setup
  public void setup()
    {
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
