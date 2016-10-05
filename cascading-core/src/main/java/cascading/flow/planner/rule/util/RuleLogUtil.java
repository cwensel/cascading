/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.planner.rule.util;

import cascading.util.LogUtil;

/**
 *
 */
public class RuleLogUtil
  {
  private static final String[] CONTEXTS = new String[]
    {
      "cascading.flow.Flow",
      "cascading.flow.planner.rule",
      "cascading.flow.planner.iso.transformer",
      "cascading.flow.planner.iso.assertion",
      "cascading.flow.planner.iso.subgraph",
      "cascading.flow.planner.iso.finder"
    };

  public static String[] enableDebugLogging()
    {
    return LogUtil.setLog4jLevel( CONTEXTS, "DEBUG" );
    }

  public static String[] enableTraceLogging()
    {
    return LogUtil.setLog4jLevel( CONTEXTS, "TRACE" );
    }

  public static String[] enableLogging( LogLevel level )
    {
    if( level == null )
      return null;

    return LogUtil.setLog4jLevel( CONTEXTS, level.toString() );
    }

  public static void restoreLogging( String[] levels )
    {
    if( levels != null )
      LogUtil.setLog4jLevel( CONTEXTS, levels );
    }
  }
