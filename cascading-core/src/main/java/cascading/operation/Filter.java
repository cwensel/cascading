/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation;

import cascading.flow.FlowProcess;

/**
 * Interface Filter marks a given {@link Operation} as a filter, as opposed to being a {@link Function}.
 * <p/>
 * A Filter is responsible for testing a Tuple to see if it should be removed from the tuple stream.
 * <p/>
 * To implement a Filter, (optionally) sub-class {@link BaseOperation} and have the new sub-class {@code implement}
 * this interface.
 */
public interface Filter<Context> extends Operation<Context>
  {
  /**
   * Method isRemove returns true if input should be removed from the tuple stream.
   *
   * @param flowProcess of type FlowProcess
   * @param filterCall  of type FilterCall
   * @return boolean
   */
  boolean isRemove( FlowProcess flowProcess, FilterCall<Context> filterCall );
  }
