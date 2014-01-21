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

package cascading.flow.planner.iso.expression;

import cascading.flow.FlowElement;

/**
 *
 */
public class FlowElementExpression extends TypeExpression<FlowElement>
  {
  public FlowElementExpression( Capture capture, boolean exact, Class<? extends FlowElement> type, Topo topo )
    {
    super( capture, exact, type, topo );
    }

  public FlowElementExpression( Capture capture, Class<? extends FlowElement> type, Topo topo )
    {
    super( capture, type, topo );
    }

  public FlowElementExpression( boolean exact, Class<? extends FlowElement> type, Topo topo )
    {
    super( exact, type, topo );
    }

  public FlowElementExpression( Class<? extends FlowElement> type, Topo topo )
    {
    super( type, topo );
    }

  public FlowElementExpression( Capture capture, boolean exact, Class<? extends FlowElement> type )
    {
    super( capture, exact, type );
    }

  public FlowElementExpression( Capture capture, Class<? extends FlowElement> type )
    {
    super( capture, type );
    }

  public FlowElementExpression( boolean exact, Class<? extends FlowElement> type )
    {
    super( exact, type );
    }

  public FlowElementExpression( Class<? extends FlowElement> type )
    {
    super( type );
    }
  }
