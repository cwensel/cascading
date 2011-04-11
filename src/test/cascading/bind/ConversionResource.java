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

import cascading.bind.tap.HTTPTap;
import cascading.bind.tap.JDBCScheme;
import cascading.bind.tap.JDBCTap;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/** A mock resource that acts as a factory for specific Tap types. */
public class ConversionResource extends TapResource<Protocol, Format>
  {
  public ConversionResource( String path, Protocol protocol, Format format )
    {
    super( path, protocol, format, SinkMode.KEEP );
    }

  public ConversionResource( String path, Protocol protocol, Format format, SinkMode mode )
    {
    super( path, protocol, format, mode );
    }

  @Override
  public Tap createTapFor( Scheme scheme )
    {
    Protocol protocol = getProtocol();

    if( protocol == null )
      protocol = Protocol.HDFS;

    switch( protocol )
      {
      case HDFS:
        return new Hfs( scheme, getIdentifier(), getMode() );
      case JDBC:
        return new JDBCTap( (JDBCScheme) scheme, getIdentifier(), getMode() );
      case HTTP:
        return new HTTPTap( scheme, getIdentifier(), getMode() );
      }

    throw new IllegalStateException( "no tap for given protocol: " + getProtocol() );
    }
  }
