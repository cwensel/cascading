/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner;

import java.util.Set;

import cascading.pipe.Pipe;

/**
 *
 */
public class Extent extends Pipe
  {
  /** Field head */
  public static final Extent head = new Extent( "head" );
  /** Field tail */
  public static final Extent tail = new Extent( "tail" );

  private Extent( String name )
    {
    super( name );
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> scopes )
    {
    return new Scope();
    }

  @Override
  public String toString()
    {
    return "[" + getName() + "]";
    }

  @Override
  public boolean equals( Object object )
    {
    if( object == null )
      return false;

    if( this == object )
      return true;

    if( object.getClass() != this.getClass() )
      return false;

    return this.getName().equals( ( (Pipe) object ).getName() );
    }
  }
