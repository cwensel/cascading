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

package cascading.stats.tez.util;

import java.util.Map;

/**
 *
 */
public class TaskStatus
  {
  String taskID;
  String status;
  long scheduledTime;
  long startTime;
  long endTime;
  String successfulAttemptID;
  Map<String, Map<String, Long>> counters;
  private String diagnostics;

  public TaskStatus( String taskID )
    {
    this.taskID = taskID;
    }

  public TaskStatus( String taskID, String status, long scheduledTime, long startTime, long endTime, String successfulAttemptID, Map<String, Map<String, Long>> counters, String diagnostics )
    {
    this.taskID = taskID;
    this.status = status;
    this.scheduledTime = scheduledTime;
    this.startTime = startTime;
    this.endTime = endTime;
    this.successfulAttemptID = successfulAttemptID;
    this.counters = counters;
    this.diagnostics = diagnostics;
    }

  public String getTaskID()
    {
    return taskID;
    }

  public String getStatus()
    {
    return status;
    }

  public long getScheduledTime()
    {
    return scheduledTime;
    }

  public long getStartTime()
    {
    return startTime;
    }

  public long getEndTime()
    {
    return endTime;
    }

  public String getSuccessfulAttemptID()
    {
    return successfulAttemptID;
    }

  public Map<String, Map<String, Long>> getCounters()
    {
    return counters;
    }

  public String getDiagnostics()
    {
    return diagnostics;
    }
  }
