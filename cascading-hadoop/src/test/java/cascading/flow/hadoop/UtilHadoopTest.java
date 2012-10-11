/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class UtilHadoopTest
  {
  @Test
  public void testSetLogLevel()
    {
    JobConf jobConf = new JobConf();

    jobConf.set( "log4j.logger", "cascading=DEBUG" );

    HadoopUtil.initLog4j( jobConf );

    Object loggerObject = Util.invokeStaticMethod( "org.apache.log4j.Logger", "getLogger",
      new Object[]{"cascading"}, new Class[]{String.class} );

    Object levelObject = Util.invokeStaticMethod( "org.apache.log4j.Level", "toLevel",
      new Object[]{"DEBUG"}, new Class[]{String.class} );

    Object returnedLevel = Util.invokeInstanceMethod( loggerObject, "getLevel",
      new Object[]{}, new Class[]{} );

    assertEquals( levelObject, returnedLevel );
    }
  }
