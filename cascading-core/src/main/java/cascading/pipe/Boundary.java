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

package cascading.pipe;

import cascading.flow.planner.Scope;
import cascading.tuple.Fields;

/**
 * The Boundary class is only used internally by the planner to mark the boundaries between partitions within
 * the element graph.
 * <p/>
 * In MapReduce, Taps are used. But in DAG models, Boundary would specify where a system dependent interface should
 * be used.
 */
public class Boundary extends Pipe
  {
  /**
   * Intentionally does not provide a chaining constructor, as Boundary should not be inserted into an assembly
   * by a user.
   */
  public Boundary()
    {
    }

  @Override
  public String getName()
    {
    return Pipe.id( this );
    }

  @Override
  public Fields resolveIncomingOperationPassThroughFields( Scope incomingScope )
    {
    return incomingScope.getIncomingFunctionPassThroughFields();
    }
  }
