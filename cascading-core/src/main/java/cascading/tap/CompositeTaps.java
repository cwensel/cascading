/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.tap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import cascading.util.Util;

/**
 * Helper utilities for {@link CompositeTap} instances.
 */
public class CompositeTaps
  {
  private CompositeTaps()
    {
    }

  public static Collection<? extends Tap> unwindNarrow( Class<? extends Tap> type, Tap tap )
    {
    return Util.narrowIdentitySet( type, unwind( tap ) );
    }

  public static Collection<Tap> unwind( Tap tap )
    {
    Set<Tap> taps = Util.createIdentitySet();

    addLeaves( tap, taps );

    return taps;
    }

  private static void addLeaves( Tap tap, Set<Tap> taps )
    {
    if( tap instanceof CompositeTap )
      {
      Iterator<? extends Tap> childTaps = ( (CompositeTap<? extends Tap>) tap ).getChildTaps();

      while( childTaps.hasNext() )
        addLeaves( childTaps.next(), taps );
      }
    else
      {
      taps.add( tap );
      }
    }
  }
