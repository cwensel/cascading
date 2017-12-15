/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

/**
 */
public enum ProcessLevel
  {
    /**
     * Applies assertions or transformations to the whole FlowElement assembly.
     */
    Assembly,

    /**
     * Applies to sub-graphs of a flow where a step is a submitted unit of cluster work.
     */
    Step,

    /**
     * Applies to generation sub-graphs of steps where is a node is a unit of processing with a step.
     * <p>
     * In MapReduce its a Map or Reduce node (where Map and Reduce are Kinds).
     * <p>
     * In Tez its a Processor.
     */
    Node,

    /**
     * Applies to sub-graphs within nodes having a single streamed input, and multiple accumulated inputs.
     * <p>
     * In MapReduce, a Mapper task can arbitrarily process a given input file via the MultiInputSplit.
     * <p>
     * Tez currently does not allow multiply physical inputs per processor.
     */
    Pipeline;

  public static ProcessLevel parent( ProcessLevel level )
    {
    if( level == Assembly )
      return null;

    return ProcessLevel.values()[ level.ordinal() - 1 ];
    }
  }
