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

import cascading.flow.hadoop.planner.rule.assertion.DualStreamedAccumulatedMergeAssert;
import cascading.flow.hadoop.planner.rule.partitioner.ConsecutiveTapsNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.ConsecutiveTapsStepPartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.GroupTapNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedAccumulatedTapsPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedOnlySourcesPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedSelfJoinSourcesPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.TapGroupNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.TapGroupTapStepPartitioner;
import cascading.flow.hadoop.planner.rule.transformer.BalanceCheckpointTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceGroupBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceGroupGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceGroupMergeGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceGroupNonBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceGroupSplitMergeGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceGroupSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceHashJoinBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceHashJoinSameSourceTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceNonSafePipeSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceNonSafeSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.BalanceSameSourceStreamedAccumulatedTransformer;
import cascading.flow.hadoop.planner.rule.transformer.CombineAdjacentTapTransformer;
import cascading.flow.hadoop.planner.rule.transformer.RemoveMalformedHashJoinTransformer;
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
//    enableDebugLogging();

    // PreBalance
    addRule( new LoneGroupAssert() );
    addRule( new MissingGroupAssert() );
    addRule( new BufferAfterEveryAssert() );
    addRule( new EveryAfterBufferAssert() );
    addRule( new SplitBeforeEveryAssert() );

    // Balance
    addRule( new BalanceGroupSplitTransformer() );
    addRule( new BalanceGroupSplitMergeGroupTransformer() );
    addRule( new BalanceGroupMergeGroupTransformer() );
    addRule( new BalanceGroupGroupTransformer() );
    addRule( new BalanceCheckpointTransformer() );
    addRule( new BalanceHashJoinSameSourceTransformer() );
    addRule( new BalanceHashJoinBlockingHashJoinTransformer() );
    addRule( new BalanceGroupBlockingHashJoinTransformer() );
    addRule( new BalanceGroupNonBlockingHashJoinTransformer() );
    addRule( new BalanceSameSourceStreamedAccumulatedTransformer() );
    addRule( new BalanceNonSafeSplitTransformer() );
    addRule( new BalanceNonSafePipeSplitTransformer() );

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

    // PartitionPipelines
    addRule( new StreamedAccumulatedTapsPipelinePartitioner() );
    addRule( new StreamedSelfJoinSourcesPipelinePartitioner() );
    addRule( new StreamedOnlySourcesPipelinePartitioner() );

    // PostPipelines
    addRule( new RemoveMalformedHashJoinTransformer() );
    addRule( new DualStreamedAccumulatedMergeAssert() );

    // enable when GraphFinder supports captured edges
//    addRule( new RemoveStreamedBranchTransformer() );

    }
  }
