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

package cascading.flow.local;

import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.planner.FlowPlanner;
import cascading.scheme.Scheme;

/**
 *
 */
public class LocalFlowConnector extends FlowConnector
  {
  public LocalFlowConnector()
    {
    }

  public LocalFlowConnector( Map<Object, Object> properties )
    {
    super( properties );
    }


  @Override
  protected Class<? extends Scheme> getDefaultIntermediateSchemeClass()
    {
    return null;
    }

  @Override
  protected FlowPlanner createFlowPlanner()
    {
    return new LocalPlanner();
    }
  }
