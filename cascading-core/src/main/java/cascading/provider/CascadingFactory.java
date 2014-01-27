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

package cascading.provider;

import cascading.flow.FlowProcess;

/**
 * Interface CascadingFactory defines a pluggable "factory" class that can be loaded by the {@link FactoryLoader}
 * utility.
 * <p/>
 * Factory instances are created process side (in a cluster for example) to augment any of the internal strategies.
 *
 * @see FactoryLoader
 */
public interface CascadingFactory<Config, Instance>
  {
  /**
   * Method initialize will configure an initialize this factory instance.
   *
   * @param flowProcess of type FlowProcess
   */
  void initialize( FlowProcess<Config> flowProcess );

  /**
   * Method create will return a new instance of the type create by this factory.
   *
   * @param flowProcess of type FlowProcess
   * @return of type Instance
   */
  Instance create( FlowProcess<Config> flowProcess );
  }
