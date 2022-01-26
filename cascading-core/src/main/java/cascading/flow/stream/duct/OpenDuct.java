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

import java.util.Iterator;

/**
 *
 */
public class OpenDuct<Incoming, Outgoing> extends Duct<Grouping<Incoming, Iterator<Incoming>>, Outgoing> implements OpenWindow
  {
  public OpenDuct( Duct<Outgoing, ?> next )
    {
    super( next );
    }

  @Override
  public void start( Duct previous )
    {
    next.start( previous );
    }

  @Override
  public void receive( Duct previous, int ordinal, Grouping<Incoming, Iterator<Incoming>> grouping )
    {
    while( grouping.joinIterator.hasNext() )
      next.receive( previous, 0, (Outgoing) grouping.joinIterator.next() );
    }

  @Override
  public void complete( Duct previous )
    {
    next.complete( previous );
    }
  }
