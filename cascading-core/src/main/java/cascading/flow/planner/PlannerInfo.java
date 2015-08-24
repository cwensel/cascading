/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.util.Objects;

/**
 *
 */
public class PlannerInfo
  {
  public static final PlannerInfo NULL = new PlannerInfo( null, null, null );

  public final String name;
  public final String platform;
  public final String registry;

  public PlannerInfo( String name, String platform, String registry )
    {
    this.name = name;
    this.platform = platform;
    this.registry = registry;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    PlannerInfo that = (PlannerInfo) object;

    return Objects.equals( name, that.name ) &&
      Objects.equals( platform, that.platform ) &&
      Objects.equals( registry, that.registry );
    }

  @Override
  public int hashCode()
    {
    return Objects.hash( name, platform, registry );
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( name ).append( ':' );
    sb.append( platform ).append( ':' );
    sb.append( registry );
    return sb.toString();
    }
  }
