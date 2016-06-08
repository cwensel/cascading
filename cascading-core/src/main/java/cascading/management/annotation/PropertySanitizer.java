/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
 * PropertySanitizer is an annotation to be used in conjunction with a Property annotation to sanitize values containing
 * sensitive information.
 * <p/>
 * For example, if a Property contains an URL, user names, password, API keys etc, one can supply a or a custom class
 * implementing the {@link Sanitizer} interface to remove or replace any sensitive data.
 * <p/>
 * The Sanitizer implementation has the option to provide a different value for each level of
 * {@link cascading.management.annotation.Visibility}.
 * <p/>
 * For example, if the raw value is an URL, the {@code PUBLIC} sanitized value may just include the URL path. The
 * {@code PROTECTED} value may retain the query string, and the {@code PRIVATE} value may retain the scheme and
 * domain name of the server.
 * <p/>
 * If a Sanitizer returns {@code null}, no value will be available for that visibility.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface PropertySanitizer
  {
  String value();
  }
