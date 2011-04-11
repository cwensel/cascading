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

package cascading.bind;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

/**
 * Class TapResource is an abstract base class to be used with the {@link Schema} class for dynamically
 * looking up Cascading {@link Tap} instances based on a given 'protocol' and 'format'.
 * <p/>
 * TapResource extends {@link Resource} and binds {@link SinkMode} as the Resource 'mode' type.
 *
 * @param <P> a 'protocol' type
 * @param <F> a data 'format' type
 */
public abstract class TapResource<P, F> extends Resource<P, F, SinkMode>
  {
  protected TapResource()
    {
    }

  public TapResource( String path, P protocol, F format, SinkMode mode )
    {
    super( path, protocol, format, mode );
    }

  public String getSimpleIdentifier()
    {
    String name = getIdentifier();

    if( name.endsWith( "/" ) )
      name = name.substring( 0, name.length() - 1 );

    return name.substring( name.lastIndexOf( "/" ) + 1 );
    }

  public abstract Tap createTapFor( Scheme scheme );
  }
