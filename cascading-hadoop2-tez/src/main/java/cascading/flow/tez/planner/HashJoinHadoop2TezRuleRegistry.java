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

package cascading.flow.tez.planner;

import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.assertion.BufferAfterEveryAssert;
import cascading.flow.planner.rule.assertion.EveryAfterBufferAssert;
import cascading.flow.planner.rule.assertion.LoneGroupAssert;
import cascading.flow.planner.rule.assertion.MissingGroupAssert;
import cascading.flow.planner.rule.assertion.SplitBeforeEveryAssert;
import cascading.flow.planner.rule.partitioner.WholeGraphStepPartitioner;
import cascading.flow.planner.rule.transformer.ApplyAssertionLevelTransformer;
import cascading.flow.planner.rule.transformer.ApplyDebugLevelTransformer;
import cascading.flow.planner.rule.transformer.RemoveNoOpPipeTransformer;
import cascading.flow.tez.planner.rule.annotator.AccumulatedPostNodeAnnotator;
import cascading.flow.tez.planner.rule.assertion.DualStreamedAccumulatedMergeNodeAssert;
import cascading.flow.tez.planner.rule.partitioner.BottomUpBoundariesNodePartitioner;
import cascading.flow.tez.planner.rule.partitioner.BottomUpJoinedBoundariesNodePartitioner;
import cascading.flow.tez.planner.rule.partitioner.ConsecutiveGroupOrMergesNodePartitioner;
import cascading.flow.tez.planner.rule.partitioner.SplitJoinBoundariesNodeRePartitioner;
import cascading.flow.tez.planner.rule.partitioner.StreamedAccumulatedBoundariesNodeRePartitioner;
import cascading.flow.tez.planner.rule.partitioner.StreamedOnlySourcesNodeRePartitioner;
import cascading.flow.tez.planner.rule.partitioner.TopDownSplitBoundariesNodePartitioner;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceCheckpointTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceGroupBlockingHashJoinTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceGroupSplitHashJoinTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceGroupSplitSpliceTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceGroupSplitTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceHashJoinSameSourceTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceHashJoinToHashJoinTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceJoinSplitTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceSplitSplitToStreamedHashJoinTransformer;
import cascading.flow.tez.planner.rule.transformer.BoundaryBalanceSplitToStreamedHashJoinTransformer;
import cascading.flow.tez.planner.rule.transformer.RemoveMalformedHashJoinNodeTransformer;

/**
 * The HashJoinHadoop2TezRuleRegistry provides support for assemblies using {@link cascading.pipe.HashJoin} pipes.
 * <p/>
 * Detecting and optimizing for HashJoin pipes adds further complexity and time to converge on a valid physical plan.
 * <p/>
 * If facing slowdowns, and no HashJoins are used, switch to the
 * {@link cascading.flow.tez.planner.NoHashJoinHadoop2TezRuleRegistry} via the appropriate
 * {@link cascading.flow.FlowConnector} constructor.
 */
public class HashJoinHadoop2TezRuleRegistry extends RuleRegistry
  {
  public HashJoinHadoop2TezRuleRegistry()
    {
//    enableDebugLogging();

    // PreBalance
    addRule( new LoneGroupAssert() );
    addRule( new MissingGroupAssert() );
    addRule( new BufferAfterEveryAssert() );
    addRule( new EveryAfterBufferAssert() );
    addRule( new SplitBeforeEveryAssert() );

    addRule( new BoundaryBalanceGroupSplitTransformer() );
    addRule( new BoundaryBalanceGroupSplitSpliceTransformer() ); // prevents AssemblyHelpersPlatformTest#testSameSourceMerge deadlock
    addRule( new BoundaryBalanceCheckpointTransformer() );

    // hash join
    addRule( new BoundaryBalanceHashJoinSameSourceTransformer() );
    addRule( new BoundaryBalanceSplitToStreamedHashJoinTransformer() ); // testGroupBySplitGroupByJoin
    addRule( new BoundaryBalanceSplitSplitToStreamedHashJoinTransformer() ); // testGroupBySplitSplitGroupByJoin
    addRule( new BoundaryBalanceHashJoinToHashJoinTransformer() ); // force HJ into unique nodes
    addRule( new BoundaryBalanceGroupBlockingHashJoinTransformer() ); // joinAfterEvery
    addRule( new BoundaryBalanceGroupSplitHashJoinTransformer() ); // groupBySplitJoins
    addRule( new BoundaryBalanceJoinSplitTransformer() ); // prevents duplication of HashJoin, testJoinSplit

    // PreResolve
    addRule( new RemoveNoOpPipeTransformer() );
    addRule( new ApplyAssertionLevelTransformer() );
    addRule( new ApplyDebugLevelTransformer() );

    // PostResolve

    // PartitionSteps
    addRule( new WholeGraphStepPartitioner() );

    // PostSteps

    // PartitionNodes

    // no match with HashJoin inclusion
    addRule( new TopDownSplitBoundariesNodePartitioner() ); // split from source to multiple sinks
    addRule( new ConsecutiveGroupOrMergesNodePartitioner() );
    addRule( new BottomUpBoundariesNodePartitioner() ); // streamed paths re-partitioned w/ StreamedOnly
    addRule( new SplitJoinBoundariesNodeRePartitioner() ); // testCoGroupSelf - compensates for tez-1190

    // hash join inclusion
    addRule( new BottomUpJoinedBoundariesNodePartitioner() ); // will capture multiple inputs into sink for use with HashJoins
    addRule( new StreamedAccumulatedBoundariesNodeRePartitioner() ); // joinsIntoCoGroupLhs & groupBySplitJoins
    addRule( new StreamedOnlySourcesNodeRePartitioner() );

    // PostNodes
    addRule( new RemoveMalformedHashJoinNodeTransformer() ); // joinsIntoCoGroupLhs
    addRule( new AccumulatedPostNodeAnnotator() ); // allows accumulated boundaries to be identified

    addRule( new DualStreamedAccumulatedMergeNodeAssert() );
    }
  }
