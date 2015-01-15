/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
