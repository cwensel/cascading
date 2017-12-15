/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
 * Constants to be used as key in {@link Flow#getFlowDescriptor()}.
 * <p>
 * See also {@link FlowStepDescriptors}.
 */
public interface FlowDescriptors
  {
  /** String that can be used as a record separator. */
  String VALUE_SEPARATOR = "\u001E";

  /** Denotes a collection  of statements like SQL or similar. */
  String STATEMENTS = "statements";

  /** Denotes the type of statement in the current flow. e.g. SQL, Pig Latin etc. */
  String STATEMENT_TYPES = "statement_types";

  /** Briefly describes the current Flow * */
  String DESCRIPTION = "description";
  }
