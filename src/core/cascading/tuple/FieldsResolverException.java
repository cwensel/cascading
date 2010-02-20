/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple;

/**
 * Class FieldsResolverException is thrown when selectorFields cannot
 * select from the sourceFields.
 */
public class FieldsResolverException extends TupleException
  {
  /** Field sourceFields */
  private Fields sourceFields;
  /** Field selectorFields */
  private Fields selectorFields;

  /**
   * Constructor FieldsResolverException creates a new FieldsResolverException instance.
   *
   * @param sourceFields   of type Fields
   * @param selectorFields of type Fields
   */
  public FieldsResolverException( Fields sourceFields, Fields selectorFields )
    {
    super( createMessage( sourceFields, selectorFields ) );

    this.sourceFields = sourceFields;
    this.selectorFields = selectorFields;
    }

  /**
   * Method getSourceFields returns the sourceFields of this FieldsResolverException object.
   *
   * @return the sourceFields (type Fields) of this FieldsResolverException object.
   */
  public Fields getSourceFields()
    {
    return sourceFields;
    }

  /**
   * Method getSelectorFields returns the selectorFields of this FieldsResolverException object.
   *
   * @return the selectorFields (type Fields) of this FieldsResolverException object.
   */
  public Fields getSelectorFields()
    {
    return selectorFields;
    }

  private static String createMessage( Fields sourceFields, Fields selectorFields )
    {
    return "could not select fields: " + selectorFields.printVerbose() + ", from: " + sourceFields.printVerbose();
    }
  }
