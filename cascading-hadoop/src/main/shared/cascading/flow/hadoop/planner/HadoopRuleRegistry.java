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

package cascading.flow.hadoop.planner;

import cascading.flow.hadoop.planner.rule.partitioner.ConsecutiveTapsNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.ConsecutiveTapsStepPartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.GroupTapNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedAccumulatedTapsNodePipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.TapGroupNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.TapGroupTapStepPartitioner;
import cascading.flow.hadoop.planner.rule.transformer.CombineAdjacentTapTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionCheckpointTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionGroupBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionGroupGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionGroupMergeGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionGroupNonBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionGroupSplitMergeGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionGroupSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionHashJoinBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionHashJoinSameSourceTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionNonSafePipeSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionNonSafeSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.PartitionSameSourceStreamedAccumulatedTransformer;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.assertion.BufferAfterEveryAssert;
import cascading.flow.planner.rule.assertion.EveryAfterBufferAssert;
import cascading.flow.planner.rule.assertion.LoneGroupAssert;
import cascading.flow.planner.rule.assertion.MissingGroupAssert;
import cascading.flow.planner.rule.assertion.SplitBeforeEveryAssert;
import cascading.flow.planner.rule.transformer.ApplyAssertionLevelTransformer;
import cascading.flow.planner.rule.transformer.ApplyDebugLevelTransformer;
import cascading.flow.planner.rule.transformer.RemoveNoOpPipeTransformer;

/**
 *
 */
public class HadoopRuleRegistry extends RuleRegistry
  {
  public HadoopRuleRegistry()
    {
    // PrePartition
    addRule( new LoneGroupAssert() );
    addRule( new MissingGroupAssert() );
    addRule( new BufferAfterEveryAssert() );
    addRule( new EveryAfterBufferAssert() );
    addRule( new SplitBeforeEveryAssert() );

    // Partition
    addRule( new PartitionGroupSplitTransformer() );
    addRule( new PartitionGroupSplitMergeGroupTransformer() );
    addRule( new PartitionGroupMergeGroupTransformer() );
    addRule( new PartitionGroupGroupTransformer() );
    addRule( new PartitionCheckpointTransformer() );
    addRule( new PartitionHashJoinSameSourceTransformer() );
    addRule( new PartitionHashJoinBlockingHashJoinTransformer() );
    addRule( new PartitionGroupBlockingHashJoinTransformer() );
    addRule( new PartitionGroupNonBlockingHashJoinTransformer() );
    addRule( new PartitionSameSourceStreamedAccumulatedTransformer() );
    addRule( new PartitionNonSafeSplitTransformer() );
    addRule( new PartitionNonSafePipeSplitTransformer() );

    // PreResolve
    addRule( new RemoveNoOpPipeTransformer() );
    addRule( new ApplyAssertionLevelTransformer() );
    addRule( new ApplyDebugLevelTransformer() );

    // PostResolve
    addRule( new CombineAdjacentTapTransformer() );

    // PartitionSteps
    addRule( new ConsecutiveTapsStepPartitioner() );
    addRule( new TapGroupTapStepPartitioner() );

    // PartitionNodes
    addRule( new ConsecutiveTapsNodePartitioner() );
    addRule( new TapGroupNodePartitioner() );
    addRule( new GroupTapNodePartitioner() );

    // PipelineNodes
    addRule( new StreamedAccumulatedTapsNodePipelinePartitioner() );
    }
  }
