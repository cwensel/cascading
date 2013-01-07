/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

  @Override
  protected boolean assertFails( Long groupSize )
    {
    return groupSize <= size;
    }
  }