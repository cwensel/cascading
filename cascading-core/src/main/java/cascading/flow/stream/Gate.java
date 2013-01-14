/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

/**
 *
 */
public abstract class Gate<Incoming, Outgoing> extends Duct<Incoming, Outgoing>
  {
  protected Duct[] allPrevious;

  public Gate()
    {
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    super.bind( streamGraph ); // sets next

    allPrevious = getAllPreviousFor( streamGraph );
    }

  /**
   * the actual previous in this context, not necessarily the declared previous
   *
   * @param streamGraph of type StreamGraph
   * @return of type Duct[]
   */
  protected Duct[] getAllPreviousFor( StreamGraph streamGraph )
    {
    return streamGraph.findAllPreviousFor( this );
    }
  }
