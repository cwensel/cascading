/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.stats;

import java.util.Map;

/**
 * Typically CascadingStats objects have an internal state model with timings, the FlowSliceStats is a simplified
 * Stats object and only reports what the underlying platform reports, not the client side observations.
 * <p/>
 * Implementations may optionally implement the {@link cascading.stats.ProvidesCounters} interface.
 * <p/>
 * Provided as an abstract class so that implementations will be resilient to API additions.
 */
public abstract class FlowSliceStats<K extends Enum>
  {
  public abstract static class FlowSliceAttempt
    {
    public abstract String getProcessAttemptID();

    public abstract int getEventId();

    public abstract int getProcessDuration();

    public abstract String getProcessStatus();

    public abstract String getStatusURL();

    public abstract CascadingStats.Status getStatus();
    }

  public abstract String getID();

  public abstract long getProcessStartTime();

  public abstract long getProcessFinishTime();

  public abstract CascadingStats.Status getStatus();

  public abstract K getKind();

  public abstract String getProcessSliceID();

  public abstract String getProcessStepID();

  public abstract String getProcessStatus();

  public abstract float getProcessProgress();

  public abstract String[] getDiagnostics();

  public abstract Map<String, Map<String, Long>> getCounters();

  public abstract Map<Integer, FlowSliceAttempt> getAttempts();
  }