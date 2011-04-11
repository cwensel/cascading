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

package cascading.flow;

import java.io.IOException;

/**
 * Class FlowSkipIfSinkExists is a {@link cascading.flow.FlowSkipStrategy} implementation that returns
 * {@code true} if the Flow sink exists, regardless if it is stale or not.
 */
public class FlowSkipIfSinkExists implements FlowSkipStrategy
  {
  public boolean skipFlow( Flow flow ) throws IOException
    {
    // if tap is in replace mode, will be treated as not existing
    long sinkModified = flow.getSinkModified();

    if( sinkModified <= 0 ) // do not skip, sinks don't exist
      return false;

    return true;
    }
  }
