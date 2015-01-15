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
 * {@link cascading.flow.Flow}s, {@link cascading.operation.Function}s and {@link cascading.operation.Filter}s.
 * <p/>
 *
 * @Property annotations can be applied to public methods and public fields, so that they can be accessed via
 * java.lang.Class#getMethods() and java.lang.Class.getFields() respectively. Adding @Property annotations on any
 * non-public method or field will have no effect.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface Property
  {
  String name();

  Visibility visibility();
  }
