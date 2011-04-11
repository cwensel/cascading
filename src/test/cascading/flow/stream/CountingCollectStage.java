/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.stream;

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
  public void receive( Duct previous, Incoming incoming )
    {
    receiveCount++;
    super.receive( previous, incoming );
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
