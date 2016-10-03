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

package cascading.flow.stream;

import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Grouping;
import cascading.flow.stream.duct.Reducing;
import cascading.flow.stream.duct.Stage;

/**
 *
 */
public class CountingCollectStage<Incoming, Outgoing> extends Stage<Incoming, Outgoing> implements Reducing<Grouping, Outgoing>
  {
  int prepareCount = 0;
  int startCount = 0;
  int startGroupCount = 0;
  int receiveCount = 0;
  int completeGroupCount = 0;
  int completeCount = 0;
  int cleanupCount = 0;

  public CountingCollectStage()
    {
    }

  public int getPrepareCount()
    {
    return prepareCount;
    }

  public int getStartCount()
    {
    return startCount;
    }

  public int getReceiveCount()
    {
    return receiveCount;
    }

  public int getCompleteCount()
    {
    return completeCount;
    }

  public int getCleanupCount()
    {
    return cleanupCount;
    }

  @Override
  public void prepare()
    {
    prepareCount++;
    super.prepare();
    }

  @Override
  public void start( Duct previous )
    {
    startCount++;
    super.start( previous );
    }

  @Override
  public void startGroup( Duct previous, Grouping grouping )
    {
    startGroupCount++;
    ( (Reducing) next ).startGroup( this, grouping );
    }

  @Override
  public void receive( Duct previous, int ordinal, Incoming incoming )
    {
    receiveCount++;
    super.receive( previous, ordinal, incoming );
    }

  @Override
  public void completeGroup( Duct previous, Outgoing outgoing )
    {
    completeGroupCount++;
    ( (Reducing) next ).completeGroup( this, outgoing );
    }

  @Override
  public void complete( Duct previous )
    {
    completeCount++;
    super.complete( previous );
    }

  @Override
  public void cleanup()
    {
    cleanupCount++;
    super.cleanup();
    }
  }
