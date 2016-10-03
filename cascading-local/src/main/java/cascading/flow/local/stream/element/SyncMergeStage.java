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

package cascading.flow.local.stream.element;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.MergeStage;
import cascading.tuple.TupleEntry;

/** A version of Merge that synchronizes the receives for local mode */
public class SyncMergeStage extends MergeStage
  {
  public SyncMergeStage( FlowProcess flowProcess, FlowElement flowElement )
    {
    super( flowProcess, flowElement );
    }

  /**
   * synchronized, as by default, each source gets its turn, no concurrent threads. Except in local mode
   *
   * @param previous
   * @param ordinal
   * @param tupleEntry
   */
  @Override
  public synchronized void receive( Duct previous, int ordinal, TupleEntry tupleEntry )
    {
    super.receive( previous, ordinal, tupleEntry );
    }
  }
