
Thanks for using Cascading.

General Information:

  Project and contact information: http://www.cascading.org/

  This distribution includes four Cascading jar files:

  cascading.jar      - all relevant Cascading class files and libraries, with a 'lib' folder
  cascading-core.jar - all Cascading Core class files
  cascading-xml.jar  - all Cascading XML sub-project class files

  To use with Hadoop, you only need the cascading.jar. We suggest merging your class files and libraries with this jar
  and executing via 'hadoop jar your.jar <your args>'. Hadoop will unpack the jar and add any libraries in 'lib' to the
  classpath.

  This ant snippet works quite well:

  <property name="cascading.home" location="${basedir}/../cascading"/>
  <property file="${cascading.home}/version.properties"/>
  <property name="cascading.release.version" value="x.y.z"/>
  <property name="cascading.filename.core" value="cascading-core-${cascading.release.version}.jar"/>
  <property name="cascading.filename.xml" value="cascading-xml-${cascading.release.version}.jar"/>
  <property name="cascading.libs" value="${cascading.home}/lib"/>
  <property name="cascading.libs.core" value="${cascading.libs}"/>
  <property name="cascading.libs.xml" value="${cascading.libs}/xml"/>

  <condition property="cascading.path" value="${cascading.home}/"
             else="${cascading.home}/build">
    <available file="${cascading.home}/${cascading.filename.core}"/>
  </condition>

  <property name="cascading.lib.core" value="${cascading.path}/${cascading.filename.core}"/>
  <property name="cascading.lib.xml" value="${cascading.path}/${cascading.filename.xml}"/>

  <target name="jar" depends="build" description="creates a Hadoop ready jar will all dependencies">

    <!-- copy Cascading classes and libraries -->
    <copy todir="${build.classes}/lib" file="${cascading.lib.core}"/>
    <copy todir="${build.classes}/lib" file="${cascading.lib.xml}"/>
    <copy todir="${build.classes}/lib">
      <fileset dir="${cascading.libs.core}" includes="*.jar"/>
      <fileset dir="${cascading.libs.xml}" includes="*.jar"/>
    </copy>

    <jar jarfile="${build.dir}/${ant.project.name}.jar">
      <fileset dir="${build.classes}"/>
      <fileset dir="${basedir}" includes="lib/"/>
      <manifest>
        <attribute name="Main-Class" value="${ant.project.name}/Main"/>
      </manifest>
    </jar>

  </target>

  Where the 'cascading.lib' property points to the cascading.jar. Remember cascading.jar already has a 'lib' folder
  embedded in it.

License:

  Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.

  Cascading is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  Cascading is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Cascading.  If not, see <http://www.gnu.org/licenses/>.

Third-party Licenses:

  All third-party licenses are included in the 'lib' folder and subfolders.