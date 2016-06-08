/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.rule;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class UnsupportedPlanException extends PlannerException
  {
  public UnsupportedPlanException()
    {
    }

  public UnsupportedPlanException( FlowElement flowElement, String message )
    {
    super( flowElement, message );
    }

  public UnsupportedPlanException( FlowElement flowElement, String message, Throwable throwable )
    {
    super( flowElement, message, throwable );
    }

  public UnsupportedPlanException( FlowElement flowElement, String message, Throwable throwable, ElementGraph elementGraph )
    {
    super( flowElement, message, throwable, elementGraph );
    }

  public UnsupportedPlanException( String string )
    {
    super( string );
    }

  public UnsupportedPlanException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  public UnsupportedPlanException( Throwable throwable )
    {
    super( throwable );
    }

  public UnsupportedPlanException( String string, ElementGraph elementGraph )
    {
    super( string, elementGraph );
    }

  public UnsupportedPlanException( String string, Throwable throwable, ElementGraph elementGraph )
    {
    super( string, throwable, elementGraph );
    }

  public UnsupportedPlanException( Rule rule, Exception exception )
    {
    super( rule, exception );
    }
  }
