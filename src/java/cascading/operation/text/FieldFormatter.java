/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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

package cascading.operation.text;

import java.util.Formatter;

import cascading.operation.Function;
import cascading.operation.Operation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/**
 * Class FieldJoiner joins the values in a Tuple with a given format and stuffs the result into a new field.
 * <p/>
 * This function uses the {@link Formatter} class for formatting the argument tuple values into a new string.
 */
public class FieldFormatter extends Operation implements Function
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "formatted";

  /** Field format */
  private String format = null;

  /**
   * Constructor FieldJoiner creates a new FieldFormatter instance using the default field name "formatted".
   *
   * @param format of type String
   */
  public FieldFormatter( String format )
    {
    super( new Fields( FIELD_NAME ) );
    this.format = format;
    }

  /**
   * Constructor FieldJoiner creates a new FieldJoiner instance.
   *
   * @param fieldDeclaration of type Fields
   * @param format           of type String
   */
  public FieldFormatter( Fields fieldDeclaration, String format )
    {
    super( fieldDeclaration );
    this.format = format;

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field name, got " + fieldDeclaration.print() );
    }

  /**
   * Method getFormat returns the format of this FieldJoiner object.
   *
   * @return the format (type String) of this FieldJoiner object.
   */
  public String getFormat()
    {
    return format;
    }

  /** @see Function#operate(TupleEntry, TupleCollector) */
  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    outputCollector.add( new Tuple( input.getTuple().format( format ) ) );
    }
  }