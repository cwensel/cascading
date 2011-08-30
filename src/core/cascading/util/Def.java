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

package cascading.util;

import java.util.Set;
import java.util.TreeSet;

import static cascading.util.Util.join;

/**
 *
 */
public class Def<T>
  {
  protected String name;
  protected Set<String> tags = new TreeSet<String>();

  public String getName()
    {
    return name;
    }

  public T setName( String name )
    {
    this.name = name;
    return (T) this;
    }

  public String getTags()
    {
    return join( tags, "," );
    }

  public T addTag( String tag )
    {
    if( tag == null || tag.isEmpty() )
      return (T) this;

    tags.add( tag );

    return (T) this;
    }

  public T addTags( String... tags )
    {
    for( String tag : tags )
      addTag( tag );

    return (T) this;
    }
  }
