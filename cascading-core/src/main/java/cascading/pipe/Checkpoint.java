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

package cascading.pipe;

/**
 * The Checkpoint pipe, if supported by the current planner, will force data to be persisted at the point in
 * the tuple stream an instance of Checkpoint is inserted into the pipe assembly.
 * <p/>
 * If a checkpoint {@link cascading.tap.Tap} is added to the {@link cascading.flow.FlowDef} via the
 * {@link cascading.flow.FlowDef#addCheckpoint(Checkpoint, cascading.tap.Tap)} method, that Tap instance
 * will be used to capture the intermediate result sets.
 * <p/>
 * It is required that any Scheme used as a checkpoint must source {@link cascading.tuple.Fields#UNKNOWN} and
 * sink {@link cascading.tuple.Fields#ALL}.
 * <p/>
 * If used with a {@link cascading.scheme.hadoop.TextDelimited} {@link cascading.scheme.Scheme} class and
 * the {@code hasHeader} value is {@code true}, a header with the resolved field names will be written to the file.
 * <p/>
 * This is especially useful for debugging complex flows.
 * <p/>
 * For the {@link cascading.flow.hadoop.HadoopFlowConnector} and Hadoop platform, a Checkpoint will force a new
 * MapReduce job ({@link cascading.flow.hadoop.HadoopFlowStep} into the {@link cascading.flow.Flow} plan.
 * <p/>
 * This can be important when used in conjunction with a {@link HashJoin} where all the operations upstream
 * from the HashJoin significantly filter out data allowing it to fit in memory.
 */
public class Checkpoint extends Pipe
  {
  /**
   * Constructor Checkpoint creates a new Checkpoint pipe which inherits the name of its previous pipe.
   *
   * @param previous of type Pipe
   */
  public Checkpoint( Pipe previous )
    {
    super( previous );
    }

  /**
   * Constructor Checkpoint creates a new Checkpoint pipe with the given name.
   *
   * @param previous of type Pipe
   */
  public Checkpoint( String name, Pipe previous )
    {
    super( name, previous );
    }
  }
