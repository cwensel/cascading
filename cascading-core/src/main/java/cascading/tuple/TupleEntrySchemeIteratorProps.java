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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.property.Props;
import cascading.util.Util;

/**
 * Class TupleEntrySchemeIteratorProps is a fluent helper class to set properties which control the behavior of the
 * {@link cascading.tuple.TupleEntrySchemeIterator}.
 */
public class TupleEntrySchemeIteratorProps extends Props
  {
  public static final String PERMITTED_EXCEPTIONS = "cascading.tuple.tupleentryiterator.exceptions.permit";

  private Class<? extends Exception>[] permittedExceptions = null;

  /**
   * Method setPermittedExceptions(  Map<Object, Object> properties, Class<? extends Exception>[] ... exceptions )
   * is used to set an array of exceptions, which are allowed to be ignored in the TupleEntySchemeInterator. If the array
   * is null, it will be ignored.
   * <p>Note that the array will be converted to a comma separated String. If you read the the property back, you can
   * convert it back to classes via the asClasses method.</p>
   *
   * @param properties a Map<Object, Object>
   * @param exceptions an array of exception classes.
   */
  public static void setPermittedExceptions( Map<Object, Object> properties, Class<? extends Exception>... exceptions )
    {
    if( exceptions != null )
      {
      List<String> classNames = new ArrayList<String>();

      for( Class<? extends Exception> clazz : exceptions )
        classNames.add( clazz.getName() );

      properties.put( PERMITTED_EXCEPTIONS, Util.join( classNames, "," ) );
      }
    }

  /**
   * Creates a new TupleEntrySchemeIteratorProps instance.
   *
   * @return TupleEntrySchemeIteratorProps instance
   */
  public static TupleEntrySchemeIteratorProps tupleEntrySchemeIteratorProps()
    {
    return new TupleEntrySchemeIteratorProps();
    }

  /**
   * Constructs a new TupleEntrySchemeIteratorProps instance.
   */
  public TupleEntrySchemeIteratorProps()
    {
    }

  public Class<? extends Exception>[] getPermittedExceptions()
    {
    return permittedExceptions;
    }

  /**
   * Method setPermittedExceptions is used to set an array of exceptions which are allowed to be ignored in the
   * TupleEntrySchemeIterator.
   * <p/>
   * If the array is null, it will be ignored.
   *
   * @param permittedExceptions an array of exception classes.
   */
  public TupleEntrySchemeIteratorProps setPermittedExceptions( Class<? extends Exception>[] permittedExceptions )
    {
    this.permittedExceptions = permittedExceptions;
    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setPermittedExceptions( properties, permittedExceptions );
    }
  }