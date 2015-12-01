/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
 * Property annotations can be used to send additional information about certain aspects of Cascading classes
 * to the {@link cascading.management.DocumentService}.  The properties are present at runtime and allow a
 * DocumentService to inspect, process or persist them.
 * <p/>
 * Property annotations can be applied to {@link cascading.tap.Tap}s, {@link cascading.scheme.Scheme}s,
 * and any {@link cascading.operation.Operation} sub-classes (like Function or Aggregator).
 * <p/>
 * Property annotations can be applied to any method or field members, so that they can be accessed via
 * java.lang.Class#getMethods() and java.lang.Class.getFields() respectively. If the member is protected/private,
 * there will be an attempt to bypass the protected/private scope to retrieve the value.
 * <p/>
 * By default the {@link Visibility} is {@link Visibility#PUBLIC}, and optionality is {@code true}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface Property
  {
  /**
   * The display name for the property value. Does not need to match the bean property or field name.
   *
   * @return the display name
   */
  String name();

  /**
   * The display visibility, see {@link Visibility}.
   *
   * @return the display visibility
   */
  Visibility visibility() default Visibility.PUBLIC;

  /**
   * Whether the field name and null should be displayed if the value is {@code null}.
   * <p/>
   * When {@code true}, no data (field name, visibility, and the {@code null} value are sent to the display. The property
   * is ignored.
   *
   * @return whether the field should be displayed if {@code null}
   */
  boolean optional() default true;
  }
