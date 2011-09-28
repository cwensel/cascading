/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.detail;

import java.util.Map;
import java.util.Properties;

import cascading.pipe.Pipe;
import cascading.test.PlatformTest;
import junit.framework.Test;
import junit.framework.TestSuite;

@PlatformTest(platforms = {"local", "hadoop"})
public class EveryEveryPipeAssemblyTest extends PipeAssemblyTestBase
  {

  public static Test suite() throws Exception
    {
    TestSuite suite = new TestSuite();

    Properties properties = loadProperties( "op.op.properties" );

    Map<String, Pipe> pipes = buildOpPipes( properties, null, new Pipe( "every.every" ), new EachAssemblyFactory(), LHS_ARGS_FIELDS, LHS_DECL_FIELDS, LHS_SELECT_FIELDS, LHS_VALUE );
    for( String name : pipes.keySet() )
      makeSuites( properties, buildOpPipes( properties, name, pipes.get( name ), new EachAssemblyFactory(), RHS_ARGS_FIELDS, RHS_DECL_FIELDS, RHS_SELECT_FIELDS, RHS_VALUE ), suite, EveryEveryPipeAssemblyTest.class );

    return suite;
    }

  public EveryEveryPipeAssemblyTest( Properties properties, String name, Pipe pipe )
    {
    super( properties, name, pipe );
    }
  }