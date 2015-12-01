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

/**
 * Visibility controls whether a certain {@link cascading.management.annotation.Property} is visible to a certain
 * audience.
 * <p/>
 * <lu>
 * <li>{@link #PRIVATE} - recommended that only a developer or team/project member have access to the value</li>
 * <li>{@link #PROTECTED} - recommended for interested and authorized parties</li>
 * <li>{@link #PUBLIC} - recommended for use as general purpose information</li>
 * </lu>
 * <p/>
 * Note {@link cascading.tap.Tap#getIdentifier()} defines the {@link cascading.management.annotation.Sanitizer}
 * implementation {@link cascading.management.annotation.URISanitizer} which attempts to cleanse the URI identifier
 * for each of the above visibilities.
 * <p/>
 * It is up to the implementation of a {@link cascading.management.DocumentService} to interpret and use these
 * values.
 * <p/>
 * Cascading does not enforce any restrictions related to the values, they are considered informative.
 * <p/>
 * To prevent serialization of any value via the registered {@link cascading.management.DocumentService}, do not
 * mark a field with the Property annotation, or configure the service appropriately.
 */
public enum Visibility
  {
    PRIVATE,
    PROTECTED,
    PUBLIC
  }
