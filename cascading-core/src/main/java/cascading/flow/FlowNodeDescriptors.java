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

package cascading.flow;

/**
 * Constants to be used as key in {@link FlowStep#getFlowStepDescriptor}.
 * <p/>
 * See also {@link FlowDescriptors}.
 */
public interface FlowNodeDescriptors
  {
  /** String that can be used as a record separator. */
  String VALUE_SEPARATOR = "\u001E";

  /** Briefly describes the current FlowStep * */
  String DESCRIPTION = "description";

  /** Code or line numbers to identify the code this node is executing */
  String TRACE = "trace";

  /** Id of the FlowStep in an external system * */
  String EXTERNAL_ID = "external_id";

  /** Name of the FlowStep in an external system * */
  String EXTERNAL_NAME = "external_name";

  /** URL of the FlowStep in an external system * */
  String EXTERNAL_URL = "external_url";

  }
