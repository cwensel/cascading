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

package cascading.flow.hadoop.planner;

import cascading.flow.hadoop.planner.rule.assertion.DualStreamedAccumulatedMergePipelineAssert;
import cascading.flow.hadoop.planner.rule.partitioner.ConsecutiveTapsNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.ConsecutiveTapsStepPartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.GroupTapNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.MultiTapGroupNodePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedAccumulatedTapsPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedOnlySourcesPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.StreamedSelfJoinSourcesPipelinePartitioner;
import cascading.flow.hadoop.planner.rule.partitioner.TapGroupTapStepPartitioner;
import cascading.flow.hadoop.planner.rule.transformer.RemoveMalformedHashJoinPipelineTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceCheckpointTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupMergeGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupNonBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupSplitJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupSplitMergeGroupTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupSplitMergeTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceGroupSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceHashJoinBlockingHashJoinTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceHashJoinSameSourceTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceNonSafePipeSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceNonSafeSplitTransformer;
import cascading.flow.hadoop.planner.rule.transformer.TapBalanceSameSourceStreamedAccumulatedTransformer;
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
public class MapReduceHadoopRuleRegistry extends RuleRegistry
  {
  public MapReduceHadoopRuleRegistry()
    {
//    enableDebugLogging();

    // PreBalance
    addRule( new LoneGroupAssert() );
    addRule( new MissingGroupAssert() );
    addRule( new BufferAfterEveryAssert() );
    addRule( new EveryAfterBufferAssert() );
    addRule( new SplitBeforeEveryAssert() );

    // Balance with temporary Taps
    addRule( new TapBalanceGroupSplitTransformer() );
    addRule( new TapBalanceGroupSplitJoinTransformer() );
    addRule( new TapBalanceGroupSplitMergeGroupTransformer() );
    addRule( new TapBalanceGroupSplitMergeTransformer() );
    addRule( new TapBalanceGroupMergeGroupTransformer() );
    addRule( new TapBalanceGroupGroupTransformer() );
    addRule( new TapBalanceCheckpointTransformer() );
    addRule( new TapBalanceHashJoinSameSourceTransformer() );
    addRule( new TapBalanceHashJoinBlockingHashJoinTransformer() );
    addRule( new TapBalanceGroupBlockingHashJoinTransformer() );
    addRule( new TapBalanceGroupNonBlockingHashJoinTransformer() );
    addRule( new TapBalanceSameSourceStreamedAccumulatedTransformer() );
    addRule( new TapBalanceNonSafeSplitTransformer() );
    addRule( new TapBalanceNonSafePipeSplitTransformer() );

    // PreResolve
    addRule( new RemoveNoOpPipeTransformer() );
    addRule( new ApplyAssertionLevelTransformer() );
    addRule( new ApplyDebugLevelTransformer() );

    // PostResolve
//    addRule( new CombineAdjacentTapTransformer() );

    // PartitionSteps
    addRule( new ConsecutiveTapsStepPartitioner() );
    addRule( new TapGroupTapStepPartitioner() );

    // PartitionNodes
    addRule( new ConsecutiveTapsNodePartitioner() );
    addRule( new MultiTapGroupNodePartitioner() );
    addRule( new GroupTapNodePartitioner() );

    // PartitionPipelines
    addRule( new StreamedAccumulatedTapsPipelinePartitioner() );
    addRule( new StreamedSelfJoinSourcesPipelinePartitioner() );
    addRule( new StreamedOnlySourcesPipelinePartitioner() );

    // PostPipelines
    addRule( new RemoveMalformedHashJoinPipelineTransformer() );

    // remove when GraphFinder supports captured edges
    addRule( new DualStreamedAccumulatedMergePipelineAssert() );

    // enable when GraphFinder supports captured edges
//    addRule( new RemoveStreamedBranchTransformer() );

    }
  }
