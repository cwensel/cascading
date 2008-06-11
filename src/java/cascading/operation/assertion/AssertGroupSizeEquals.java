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

package cascading.operation.assertion;

/** Class AssertGroupSizeEquals is an {@link cascading.operation.ValueAssertion} that asserts the number of items in the current group. */
public class AssertGroupSizeEquals extends AssertGroupBase
  {

  /**
   * Constructor AssertGroupSizeEquals creates a new AssertGroupSizeEquals instance.
   *
   * @param size of type long
   */
  public AssertGroupSizeEquals( long size )
    {
    super( "group size %s, is not equal to: %s, in group %s: %s", size );
    }

  protected boolean compare( Long groupSize )
    {
    return groupSize != size;
    }
  }