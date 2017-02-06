/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tuple;

/**
 * Class FieldsResolverException is thrown when selectorFields cannot
 * select from the sourceFields.
 */
public class FieldsResolverException extends TupleException
  {
  /** Field sourceFields */
  private final Fields sourceFields;
  /** Field selectorFields */
  private final Fields selectorFields;

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
