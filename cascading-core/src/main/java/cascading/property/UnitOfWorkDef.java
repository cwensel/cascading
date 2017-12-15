/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.property;

import java.util.Set;
import java.util.TreeSet;

import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.join;

/** Class UnitOfWorkDef is the base class for framework specific fluent definition interfaces. */
public class UnitOfWorkDef<T>
  {
  private static final Logger LOG = LoggerFactory.getLogger( UnitOfWorkDef.class );

  protected String name;
  protected Set<String> tags = new TreeSet<String>();

  public UnitOfWorkDef()
    {
    }

  protected UnitOfWorkDef( UnitOfWorkDef<T> unitOfWorkDef )
    {
    this.name = unitOfWorkDef.name;
    this.tags.addAll( unitOfWorkDef.tags );
    }

  public String getName()
    {
    return name;
    }

  /**
   * Method setName sets the UnitOfWork name.
   *
   * @param name type String
   * @return this
   */
  public T setName( String name )
    {
    this.name = name;
    return (T) this;
    }

  public String getTags()
    {
    return join( tags, "," );
    }

  /**
   * Method addTag will associate a "tag" with this UnitOfWork. A UnitOfWork can have an unlimited number of tags.
   * <p>
   * Tags allow for search and organization by management tools.
   * <p>
   * Tag values are opaque, but adopting a simple convention of 'category:value' allows for complex use cases.
   * <p>
   * Note that tags should not contain whitespace characters, even though this is not an error, a warning will be
   * issues.
   *
   * @param tag type String
   * @return this
   */
  public T addTag( String tag )
    {
    if( tag == null || tag.isEmpty() )
      return (T) this;

    tag = tag.trim();

    if( Util.containsWhitespace( tag ) )
      LOG.warn( "tags should not contain whitespace characters: '{}'", tag );

    tags.add( tag );

    return (T) this;
    }

  /**
   * Method addTags will associate the given "tags" with this UnitOfWork. A UnitOfWork can have an unlimited number of tags.
   * <p>
   * Tags allow for search and organization by management tools.
   * <p>
   * Tag values are opaque, but adopting a simple convention of 'category:value' allows for complex use cases.
   * <p>
   * Note that tags should not contain whitespace characters, even though this is not an error, a warning will be
   * issues.
   *
   * @param tags type String
   * @return this
   */
  public T addTags( String... tags )
    {
    for( String tag : tags )
      addTag( tag );

    return (T) this;
    }
  }
