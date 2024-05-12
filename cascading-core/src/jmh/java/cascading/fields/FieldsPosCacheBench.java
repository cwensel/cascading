/*
 * Copyright (c) 2007-2024 The Cascading Authors. All Rights Reserved.
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

package cascading.fields;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import cascading.tuple.Fields;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Warmup(iterations = 1, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 250, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FieldsPosCacheBench
  {
  private static Comparable[] comparables = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"};
  private static Fields fieldsUntyped = new Fields( comparables );

  private static Fields[] fields;
  private static Comparable<?>[][] elements;

  static
    {
    fields = new Fields[ comparables.length ];
    elements = new Comparable[ comparables.length ][];
    for( int i = 0; i < comparables.length; i++ )
      {
      elements[ i ] = Arrays.copyOf( comparables, i + 1 );
      fields[ i ] = new Fields( elements[ i ] );
      }
    }

  @Param({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"})
  private int index;

  /**
   * using IdentityHashMap
   * # Benchmark: cascading.fields.FieldsPosCacheBench.containsIdentity
   * # Parameters: (index = 14)
   * Result "cascading.fields.FieldsPosCacheBench.containsIdentity":
   *   3.744 ±(99.9%) 0.200 ns/op [Average]
   *   (min, avg, max) = (3.705, 3.744, 3.824), stdev = 0.052
   *   CI (99.9%): [3.545, 3.944] (assumes normal distribution)
   *
   * using HashMap
   * Result "cascading.fields.FieldsPosCacheBench.containsIdentity":
   *   3.758 ±(99.9%) 0.178 ns/op [Average]
   *   (min, avg, max) = (3.719, 3.758, 3.826), stdev = 0.046
   *   CI (99.9%): [3.579, 3.936] (assumes normal distribution)
   *
   * using WeakHashMap
   * Result "cascading.fields.FieldsPosCacheBench.containsIdentity":
   *   3.938 ±(99.9%) 0.202 ns/op [Average]
   *   (min, avg, max) = (3.908, 3.938, 4.032), stdev = 0.053
   *   CI (99.9%): [3.736, 4.141] (assumes normal distribution)
   *
   * @param bh
   */
  @Benchmark
  public void containsIdentity( Blackhole bh )
    {
    bh.consume( fieldsUntyped.contains( fields[ index ] ) );
    }

  /**
   * using IdentityHashMap
   * # Benchmark: cascading.fields.FieldsPosCacheBench.containsEquals
   * # Parameters: (index = 14)
   * Result "cascading.fields.FieldsPosCacheBench.containsEquals":
   *   549.168 ±(99.9%) 288.971 ns/op [Average]
   *   (min, avg, max) = (493.761, 549.168, 673.781), stdev = 75.045
   *   CI (99.9%): [260.197, 838.139] (assumes normal distribution)
   *
   * using HashMap
   * Result "cascading.fields.FieldsPosCacheBench.containsEquals":
   *   143.050 ±(99.9%) 13.049 ns/op [Average]
   *   (min, avg, max) = (141.058, 143.050, 149.062), stdev = 3.389
   *   CI (99.9%): [130.001, 156.099] (assumes normal distribution)
   *
   * using WeakHashMap
   * Result "cascading.fields.FieldsPosCacheBench.containsEquals":
   *   146.248 ±(99.9%) 1.322 ns/op [Average]
   *   (min, avg, max) = (145.972, 146.248, 146.766), stdev = 0.343
   *   CI (99.9%): [144.926, 147.571] (assumes normal distribution)
   *
   * @param bh
   */
  @Benchmark
  public void containsEquals( Blackhole bh )
    {
    Fields value = new Fields( elements[ index ] );
    bh.consume( fieldsUntyped.contains( value ) );
    }
  }
