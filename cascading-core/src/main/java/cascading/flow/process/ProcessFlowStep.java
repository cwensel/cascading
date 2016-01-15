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

package cascading.flow.process;

import java.util.Collections;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import riffle.process.scheduler.ProcessWrapper;

public class ProcessFlowStep extends BaseFlowStep
  {
  public ProcessFlowStep( ProcessWrapper processWrapper, int counter )
    {
    super( processWrapper.toString(), counter );
    }

  public ProcessFlowStep( ProcessWrapper processWrapper, int counter, Map<String, String> flowStepDescriptor )
    {
    super( processWrapper.toString(), counter, flowStepDescriptor );
    }

  @Override
  public Map<Object, Object> getConfigAsProperties()
    {
    return Collections.emptyMap();
    }

  @Override
  public Object createInitializedConfig( FlowProcess flowProcess, Object parentConfig )
    {
    return null;
    }

  @Override
  public void clean( Object object )
    {

    }

  @Override
  protected FlowStepJob createFlowStepJob( ClientState clientState, FlowProcess flowProcess, Object initializedStepConfig )
    {
    return null;
    }

  }
