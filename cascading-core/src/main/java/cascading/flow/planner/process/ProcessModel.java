/*
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

package cascading.flow.planner.process;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.pipe.Group;
import cascading.tap.Tap;

/**
 *
 */
public interface ProcessModel
  {
  String getID();

  int getOrdinal();

  String getName();

  /**
   * Method getProcessAnnotations returns an immutable Map of platform specific annotations or meta-data
   * that describe the current model type.
   * <p/>
   * Use {@link #addProcessAnnotation(Enum)} or {@link #addProcessAnnotation(String, String)} to insert
   * annotations or meta-data.
   *
   * @return an immutable Map
   */
  Map<String, String> getProcessAnnotations();

  void addProcessAnnotation( Enum annotation );

  void addProcessAnnotation( String key, String value );

  Collection<Group> getGroups();

  Set<Tap> getSourceTaps();

  Set<Tap> getSinkTaps();

  int getSubmitPriority();

  Set<FlowElement> getSinkElements();

  Set<FlowElement> getSourceElements();

  Map<String, Tap> getTrapMap();

  ElementGraph getElementGraph();
  }
