/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.detail;

import java.util.Map;
import java.util.Properties;

import cascading.pipe.Pipe;
import junit.framework.Test;
import junit.framework.TestSuite;

/** @version : IntelliJGuide,v 1.13 2001/03/22 22:35:22 SYSTEM Exp $ */
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