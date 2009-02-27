
Thanks for using Cascading.

General Information:

  Project and contact information: http://www.cascading.org/

  This distribution includes four Cascading jar files:

  cascading-x.y.z.jar      - all relevant Cascading class files and libraries, with a 'lib' folder
  cascading-core-x.y.z.jar - all Cascading Core class files
  cascading-xml-x.y.z.jar  - all Cascading XML operations class files
  cascadgin-test-x.y.z.jar - all Cascading tests and test utilities

  To use with Hadoop, we suggest stuffing cascading-core and cascading-xml jar files, and all third-party libs
  into the 'lib' folder of your job jar and executing via 'hadoop jar your.jar <your args>'.

  For example, your job jar would look like this (via: jar -t your.jar)

    /<all your class and resource files>
    /lib/cascading-core-x.y.z.jar
    /lib/cascading-xml-x.y.z.jar
    /lib/<cascading third-party jar files>

  Hadoop will unpack the jar locally and remotely (in the cluster) and add any libraries in 'lib' to the classpath.
  This is a feature specific to Hadoop.

  The cascading-x.y.z.jar file is typically used with scripting languages and is completely self contained.

  This ant snippet works quite well (you may need to override cascading.home):

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