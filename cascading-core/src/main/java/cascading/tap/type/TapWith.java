/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.tap.type;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

/**
 * Interface TapWith provides a set of methods to support fluent like calls to construct new immutable instances
 * of the the implementing class.
 * <p>
 * Each method is a factory for creating a new instance where the given parameter overrides the existing value.
 * <p>
 * Thus to open a file underneath the directory this Tap (that implements {@link FileType}) represents, call
 * {@link #withChildIdentifier(String)} to get a new Tap instance that will resolve the relative path with the current
 * path and return a new instance.
 */
public interface TapWith<Config, Input, Output>
  {
  /**
   * Method withScheme returns a new instance using the given scheme.
   *
   * @param scheme of type Scheme
   * @return a new instance of this class cast to the TapWith interface
   */
  TapWith<Config, Input, Output> withScheme( Scheme<Config, Input, Output, ?, ?> scheme );

  /**
   * Method withChildIdentifier returns a new instance using the given identifier value.
   * <p>
   * If the current identifier and the given identifier have no common base, the given identifier is resolved
   * against the current identifier.
   *
   * @param identifier of type String
   * @return a new instance of this class cast to the TapWith interface
   */
  TapWith<Config, Input, Output> withChildIdentifier( String identifier );

  /**
   * Method withSinkMode retuns a new instance using the given sinkMode value.
   *
   * @param sinkMode of type SinkMode
   * @return a new instance of this class cast to the TapWith interface
   */
  TapWith<Config, Input, Output> withSinkMode( SinkMode sinkMode );

  /**
   * Method asTap is a convenience method that returns this instance cast to the Tap class.
   * <p>
   * No new instances are created.
   *
   * @return returns this instance cast to the Tap class
   */
  default Tap<Config, Input, Output> asTap()
    {
    return (Tap<Config, Input, Output>) this;
    }
  }