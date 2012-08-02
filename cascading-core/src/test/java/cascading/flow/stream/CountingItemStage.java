/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
public class CountingItemStage<Incoming, Outgoing> extends Stage<Incoming, Outgoing> implements Mapping
  {
  int prepareCount = 0;
  int receiveCount = 0;
  int cleanupCount = 0;

  public CountingItemStage()
    {
    }

  public int getPrepareCount()
    {
    return prepareCount;
    }

  public int getReceiveCount()
    {
    return receiveCount;
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
  public void receive( Duct previous, Incoming incoming )
    {
    receiveCount++;
    super.receive( previous, incoming );
    }

  @Override
  public void cleanup()
    {
    cleanupCount++;
    super.cleanup();
    }
  }
