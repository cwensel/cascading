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

package cascading.flow.hadoop.planner.rule.partitioner;

import cascading.flow.hadoop.planner.rule.expression.StreamedAccumulatedTapsHashJoinPipelinePartitionExpression;
import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.rule.partitioner.ExpressionRulePartitioner;
import cascading.flow.stream.annotations.StreamMode;

import static cascading.flow.planner.rule.PlanPhase.PartitionPipelines;

/**
 * This rule accounts for a Tap marked as both streamed and accumulated, but is streamed in one pipeline and
 * accumulated in another pipeline.
 * <p>
 * The rule leverages the ability to not capture some matched edges, that represent paths that should not be included
 * in the result pipeline.
 */
public class StreamedAccumulatedTapsHashJoinPipelinePartitioner extends ExpressionRulePartitioner
  {
  public StreamedAccumulatedTapsHashJoinPipelinePartitioner()
    {
    super(
      PartitionPipelines,
      new StreamedAccumulatedTapsHashJoinPipelinePartitionExpression(),
      new ElementAnnotation( ElementCapture.Primary, StreamMode.Streamed ),
      new ElementAnnotation( ElementCapture.Include, StreamMode.Accumulated )
    );
    }
  }
