/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
    sb.append( "PlatformInfo" );
    sb.append( "{name='" ).append( name ).append( '\'' );
    sb.append( ", vendor='" ).append( vendor ).append( '\'' );
    sb.append( ", version='" ).append( version ).append( '\'' );
    sb.append( '}' );
    return sb.toString();
    }
  }
