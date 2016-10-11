/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.planner.rule.partitioner;

import cascading.flow.planner.iso.subgraph.partitioner.WholeGraphPartitioner;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RulePartitioner;
import cascading.flow.planner.rule.util.LogLevel;

/**
 *
 */
public class WholeGraphRulePartitioner extends RulePartitioner
  {
  protected WholeGraphRulePartitioner( LogLevel logLevel, PlanPhase phase )
    {
    super( logLevel, phase, PartitionSource.PartitionParent, new WholeGraphPartitioner() );
    }

  protected WholeGraphRulePartitioner( PlanPhase phase )
    {
    super( phase, PartitionSource.PartitionParent, new WholeGraphPartitioner() );
    }

  @Override
  public Enum[] getAnnotationExcludes()
    {
    return new Enum[ 0 ];
    }
  }
