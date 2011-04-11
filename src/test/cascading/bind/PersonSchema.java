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

import cascading.bind.tap.JDBCScheme;
import cascading.bind.tap.JSONScheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tuple.Fields;

/**
 * A mock Schema that represents a 'person' type.
 * <p/>
 * Note the constant fields which can be used inside a Cascading application.
 */
public class PersonSchema extends Schema<Protocol, Format>
  {
  public static final Fields FIRST = new Fields( "firstName" );
  public static final Fields LAST = new Fields( "lastName" );
  public static final Fields ADDRESS = new Fields( "address" );

  public static final Fields FIELDS = FIRST.append( LAST ).append( ADDRESS );

  public PersonSchema()
    {
    super( Protocol.HDFS );

    addSchemeFor( Format.TSV, new TextDelimited( FIELDS, "\t" ) );
    addSchemeFor( Format.CSV_HEADERS, new TextDelimited( FIELDS, true, ",", "\"" ) );
    addSchemeFor( Format.CSV, new TextDelimited( FIELDS, ",", "\"" ) );
    addSchemeFor( Format.Native, new SequenceFile( FIELDS ) );

    addSchemeFor( Protocol.JDBC, Format.Native, new JDBCScheme( FIELDS ) );
    addSchemeFor( Protocol.HTTP, Format.JSON, new JSONScheme( FIELDS ) );
    addSchemeFor( Protocol.HTTP, Format.TSV, new TextDelimited( FIELDS, "\t" ) );
    }
  }
