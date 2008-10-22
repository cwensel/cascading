/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe;

/**
 * *** Experimental - May be removed in a future iteration ***
 * <p/>
 * Class EndPipe marks the end of a branch in a pipe assembly. Strictly this is not necessary, but it may allow
 * for tagging meta-data to a Tap instance that comes immediately after it. It will also allow for branching
 * after the Group to be handled in the reducer so that a reducer can write out to multiple output files.
 * <p/>
 * Currently this tagging is used to mark a tap to by pass the default OutputCollector and to write 'directly' via
 * a Tap TapCollector.
 */
public class EndPipe extends Pipe
  {
  public EndPipe( String name, Pipe previous )
    {
    super( name, previous );
    }

  public EndPipe( Pipe previous )
    {
    super( previous );
    }
  }
