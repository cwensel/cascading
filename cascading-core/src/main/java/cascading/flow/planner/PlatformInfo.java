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

package cascading.flow.planner;

import java.io.Serializable;

/**
 *
 */
public class PlatformInfo implements Serializable, Comparable
  {
  public final String name;
  public final String vendor;
  public final String version;

  public PlatformInfo( String name, String vendor, String version )
    {
    this.name = name;
    this.vendor = vendor;
    this.version = version;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    PlatformInfo that = (PlatformInfo) object;

    if( name != null ? !name.equals( that.name ) : that.name != null )
      return false;
    if( vendor != null ? !vendor.equals( that.vendor ) : that.vendor != null )
      return false;
    if( version != null ? !version.equals( that.version ) : that.version != null )
      return false;

    return true;
    }

  @Override
  public int compareTo( Object o )
    {
    if( name == null )
      return -1;

    return name.compareTo( ( (PlatformInfo) o ).name );
    }

  @Override
  public int hashCode()
    {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + ( vendor != null ? vendor.hashCode() : 0 );
    result = 31 * result + ( version != null ? version.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();

    sb.append( name ).append( ':' );

    if( version != null )
      sb.append( version ).append( ':' );

    if( vendor != null )
      sb.append( vendor );

    return sb.toString();
    }
  }
