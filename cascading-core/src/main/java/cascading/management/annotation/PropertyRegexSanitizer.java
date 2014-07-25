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

package cascading.management.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * PropertyRegexSanitizer is an annotation to be used in conjunction with a Property annotation to sanitize values containing
 * sensitive information with a given regular expression.
 * <p/>
 * For example, if a Property contains an URL, user names, password, API keys etc, one can supply a regular expression
 * (regex) to remove the sensitive parts.
 * <p/>
 * Unlike the {@link cascading.management.annotation.PropertySanitizer} annotation, the regular expression is applied
 * once and the result is stored as the value for the declared {@link cascading.management.annotation.Visibility}
 * for the property. See PropertySanitizer if different values should be returned for each Visibility type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface PropertyRegexSanitizer
  {
  String value();
  }
