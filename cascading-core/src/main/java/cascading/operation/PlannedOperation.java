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

package cascading.operation;

/**
 * Interface PlannedOperation is implemented by all {@link Operation} implementations
 * that use a {@link PlannerLevel} value to inform the {@link cascading.flow.planner.FlowPlanner} how to treat the operation
 * during job planning.
 */
public interface PlannedOperation<Context> extends Operation<Context>
  {
  boolean supportsPlannerLevel( PlannerLevel plannerLevel );
  }