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

package cascading.detail;

import java.util.Properties;

import cascading.pipe.Pipe;
import cascading.platform.TestPlatform;
import junit.framework.Test;
import junit.framework.TestSuite;

public class EachPipeAssemblyPlatformTest extends PipeAssemblyTestBase
  {
  public static Test suite( TestPlatform testPlatform ) throws Exception
    {
    TestSuite suite = new TestSuite();

    Properties properties = loadProperties( "op.properties" );
    makeSuites( testPlatform, properties, buildOpPipes( null, new Pipe( "each" ), new EachAssemblyFactory(), OP_ARGS_FIELDS, OP_DECL_FIELDS, OP_SELECT_FIELDS, OP_VALUE, runOnly( properties ) ), suite, EachPipeAssemblyPlatformTest.class );

    return suite;
    }

  public EachPipeAssemblyPlatformTest( Properties properties, String displayName, String name, Pipe pipe )
    {
    super( properties, displayName, name, pipe );
    }
  }
