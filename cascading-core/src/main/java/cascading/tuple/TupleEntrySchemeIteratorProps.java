/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.property.Props;
import cascading.util.Util;

/**
 * Class TupleEntrySchemeIteratorProps is a fluent helper class to set properties which control the behavior of the
 * {@link cascading.tuple.TupleEntrySchemeIterator}.
 */
public class TupleEntrySchemeIteratorProps extends Props
  {
  public static final String PERMITTED_EXCEPTIONS = "cascading.tuple.tupleentryiterator.exceptions.permit";

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

  private Class<? extends Exception>[] exceptions = null;

  /**
   * Constructs a new TupleEntrySchemeIteratorProps instance.
   */
  public TupleEntrySchemeIteratorProps()
    {
    }

  public static TupleEntrySchemeIteratorProps tupleEntrySchemeIteratorProps()
    {
    return new TupleEntrySchemeIteratorProps();
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setPermittedExceptions( properties, exceptions );
    }

  public Class<? extends Exception>[] getExceptions()
    {
    return exceptions;
    }

  public TupleEntrySchemeIteratorProps setExceptions( Class<? extends Exception>[] exceptions )
    {
    this.exceptions = exceptions;
    return this;
    }
  }