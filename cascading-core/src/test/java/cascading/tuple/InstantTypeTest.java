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

package cascading.tuple;

import java.time.Instant;

import cascading.CascadingTestCase;
import cascading.tuple.type.InstantType;
import org.junit.Test;

public class InstantTypeTest extends CascadingTestCase
  {
  @Test
  public void coerceToString()
    {
    assertEquals( "2023-06-12T21:29:58Z", InstantType.ISO_MILLIS.coerce( Instant.ofEpochSecond( 1686605398L ), String.class ) );
    assertEquals( "2023-06-12T21:29:58.001Z", InstantType.ISO_MILLIS.coerce( Instant.ofEpochMilli( 1686605398001L ), String.class ) );
    assertEquals( "2023-06-12T21:29:58.001Z", InstantType.ISO_MILLIS.coerce( Instant.ofEpochSecond( 1686605398L, 1_000_000 ), String.class ) );
    assertEquals( "2023-06-12T21:29:58.000001Z", InstantType.ISO_MICROS.coerce( Instant.ofEpochSecond( 1686605398L, 1_000 ), String.class ) );
    assertEquals( "2023-06-12T21:29:58.000000001Z", InstantType.ISO_NANOS.coerce( Instant.ofEpochSecond( 1686605398L, 1 ), String.class ) );
    }

  @Test
  public void coerceFromString()
    {
    assertEquals( Instant.ofEpochSecond( 1686605398L ), InstantType.ISO_MILLIS.canonical( "2023-06-12T21:29:58Z" ) );
    assertEquals( Instant.ofEpochMilli( 1686605398001L ), InstantType.ISO_MILLIS.canonical( "2023-06-12T21:29:58.001Z" ) );
    assertEquals( Instant.ofEpochSecond( 1686605398L, 1_000_000 ), InstantType.ISO_MILLIS.canonical( "2023-06-12T21:29:58.001Z" ) );
    assertEquals( Instant.ofEpochSecond( 1686605398L, 1_000 ), InstantType.ISO_MICROS.canonical( "2023-06-12T21:29:58.000001Z" ) );
    assertEquals( Instant.ofEpochSecond( 1686605398L, 1 ), InstantType.ISO_NANOS.canonical( "2023-06-12T21:29:58.000000001Z" ) );
    }
  }
