/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 *
 */
@State(Scope.Benchmark)
public class CoerceBench
  {
  Type[] canonicalTypes = new Type[]{
    String.class,
    Integer.class,
    Integer.TYPE,
    Double.class,
    Double.TYPE
  };

  CoercibleType[] coercibleTypes = Coercions.coercibleArray( canonicalTypes.length, canonicalTypes );

  Object[][] values = new Object[][]{
    {null, null, 0, null, 0D},
    {"1000.000", 1000, 1000, 1000.000D, 1000.000D},
    };

  @BenchmarkMode({Mode.Throughput})
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public void baseline( Blackhole bh )
    {
    for( Object[] list : values )
      {
      for( int i = 0; i < coercibleTypes.length; i++ )
        {
        bh.consume( coercibleTypes[ i ].coerce( list[ i ], canonicalTypes[ i ] ) );
        }
      }
    }
  }
