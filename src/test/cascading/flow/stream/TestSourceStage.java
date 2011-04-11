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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class TestSourceStage<Outgoing> extends Stage<Void, Outgoing>
  {
  List<Outgoing> values = new ArrayList<Outgoing>();

  public TestSourceStage( Collection<Outgoing> collection )
    {
    values.addAll( collection );
    }

  @Override
  public void receive( Duct previous, Void value )
    {
    next.start( this );

    for( Outgoing item : values )
      next.receive( this, item );

    next.complete( this );
    }
  }
