/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.flow.stream.duct;

/**
 *
 */
public class CloseReducingDuct<Group, Incoming, Outgoing> extends Duct<Incoming, Outgoing> implements CloseWindow, Reducing<Group, Outgoing>
  {
  public CloseReducingDuct( Duct<Outgoing, ?> next )
    {
    super( next );
    }

  @Override
  public void receive( Duct previous, int ordinal, Incoming incoming )
    {
    // do nothing
    }

  @Override
  public void startGroup( Duct previous, Group group )
    {
    }

  @Override
  public void completeGroup( Duct previous, Outgoing outgoing )
    {
    next.receive( previous, 0, outgoing );
    }
  }
