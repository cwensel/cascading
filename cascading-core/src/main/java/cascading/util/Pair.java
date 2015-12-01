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

package cascading.util;

/**
 *
 */
public class Pair<Lhs, Rhs>
  {
  Lhs lhs;
  Rhs rhs;

  public Pair( Lhs lhs, Rhs rhs )
    {
    this.lhs = lhs;
    this.rhs = rhs;
    }

  public Lhs getLhs()
    {
    return lhs;
    }

  public void setLhs( Lhs lhs )
    {
    this.lhs = lhs;
    }

  public Rhs getRhs()
    {
    return rhs;
    }

  public void setRhs( Rhs rhs )
    {
    this.rhs = rhs;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Pair pair = (Pair) object;

    if( lhs != null ? !lhs.equals( pair.lhs ) : pair.lhs != null )
      return false;
    if( rhs != null ? !rhs.equals( pair.rhs ) : pair.rhs != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = lhs != null ? lhs.hashCode() : 0;
    result = 31 * result + ( rhs != null ? rhs.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "Pair{" );
    sb.append( "lhs=" ).append( lhs );
    sb.append( ", rhs=" ).append( rhs );
    sb.append( '}' );
    return sb.toString();
    }
  }
