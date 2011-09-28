/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
