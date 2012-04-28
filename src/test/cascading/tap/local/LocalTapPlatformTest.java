/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.local;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.Serializable;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 *
 */
@PlatformRunner.Platform({LocalPlatform.class})
public class LocalTapPlatformTest extends PlatformTestCase implements Serializable
  {

  @Test
  public void testIO()
    {
    String lines = "line1\nline2\n";
    System.setIn( new ByteArrayInputStream( lines.getBytes() ) );
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    System.setOut( new PrintStream( output ) );

    Tap source = new StdInTap( new TextLine( new Fields( "line" ) ) );
    Tap sink = new StdOutTap( new TextLine( new Fields( "line" ) ) );

    Pipe pipe = new Pipe( "io" );

    Flow flow = new LocalFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertEquals( lines, output.toString() );
    }
  }
