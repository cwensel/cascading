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

package cascading.operation.assertion;

import java.beans.ConstructorProperties;

/**
 * Class AssertGroupSizeEquals is an {@link cascading.operation.GroupAssertion} that asserts the number of items in the current group
 * is more than the given size.
 * </p>
 * If a patternString is given, only grouping keys that match the regular expression will have this assertion applied.
 * Note multiple key values will be delimited by a tab character.
 */

public class AssertGroupSizeMoreThan extends AssertGroupBase
  {

  /**
   * Constructor AssertGroupSizeMoreThan creates a new AssertGroupSizeMoreThan instance.
   *
   * @param size of type long
   */
  @ConstructorProperties({"size"})
  public AssertGroupSizeMoreThan( long size )
    {
    super( "group size %s, is less than or equal to: %s, in group %s: %s", size );
    }

  /**
   * Constructor AssertGroupSizeMoreThan creates a new AssertGroupSizeMoreThan instance.
   *
   * @param patternString of type String
   * @param size          of type long
   */
  @ConstructorProperties({"patternString", "size"})
  public AssertGroupSizeMoreThan( String patternString, long size )
    {
    super( "group matching '%s' with size: %s, is less than or equal to: %s, in group %s: %s", patternString, size );
    }

  protected boolean assertFails( Long groupSize )
    {
    return groupSize <= size;
    }
  }