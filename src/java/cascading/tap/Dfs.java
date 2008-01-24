/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

package cascading.tap;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/** Class Dfs is a {@link Tap} class that provides access to the Hadoop Distributed File System. */
public class Dfs extends Hfs
  {
  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param sourceFields of type Fields
   * @param stringPath   of type String
   */
  public Dfs( Fields sourceFields, String stringPath )
    {
    super( sourceFields, stringPath );
    }

  Dfs( Scheme scheme )
    {
    super( scheme );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   */
  public Dfs( Scheme scheme, String stringPath )
    {
    super( scheme, stringPath );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param scheme       of type Scheme
   * @param stringPath   of type String
   * @param deleteOnInit of type boolean
   */
  public Dfs( Scheme scheme, String stringPath, boolean deleteOnInit )
    {
    super( scheme, stringPath, deleteOnInit );
    }
  }
