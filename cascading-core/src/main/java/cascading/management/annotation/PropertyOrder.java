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
 * PropertyOrder is an annotation at the class level that controls the sort order of the Property annotations present
 * on that class.
 * </p>
 * Order.DECLARED is the default sort order in which the properties are returned in the order they are returned by the
 * reflection API of the JVM. This can be implementation specific and is not guaranteed to be stable.
 * <p/>
 * Using Order.ALPHABETICAL will cause the properties to be sorted alphabetically.
 * <p/>
 * Order.GIVEN and an array of Strings containing <em>all</em> properties of that class, will result in a custom order.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PropertyOrder
  {
  Order order() default Order.DECLARED;

  String[] properties() default {};
  }
