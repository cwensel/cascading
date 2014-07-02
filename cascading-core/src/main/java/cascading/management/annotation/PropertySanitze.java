/*
 * Copyright (c) 2007-2017 Concurrent, Inc. All Rights Reserved.
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

package cascading.management.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * PropertySanitize is an annotation to be used in conjunction with a Property annotation to sanitize values for a certain
 * visibility. This can be useful if a Property contains usernames, password, API keys etc. One can either supply a regex
 * or a custom class implementing the {@link Sanitize} interface.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface PropertySanitze
  {
  // special null object, since null is not allowed as a default in an annotation
  static class None implements Sanitize
    {
    @Override
    public String apply( Visibility visibility, String value )
      {
      return null;
      }
    }

  Visibility visibility();

  String regex() default "";

  Class<? extends Sanitize> sanitize() default None.class;

  }
