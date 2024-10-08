#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Start from Java 11 based on Ubuntu Focal Linux image (there is no Alpine Linux image with Java 11 on ARM64)
ARG JDK_VERSION=11
FROM eclipse-temurin:${JDK_VERSION}-jre-focal

# Settings
ENV IGNITE_HOME /opt/ignite/apache-ignite
WORKDIR /opt/ignite

# Copy main binary archive
COPY apache-ignite* apache-ignite

# Copy sh files and set permission
COPY run.sh $IGNITE_HOME/

# Grant permission to copy optional libs
RUN chmod 777 ${IGNITE_HOME}/libs

# Grant permission to create work directory
RUN chmod 777 ${IGNITE_HOME}

# Grant permission to execute entry point
RUN chmod 555 $IGNITE_HOME/run.sh

# Entry point
CMD $IGNITE_HOME/run.sh

# Container port exposure
EXPOSE 11211 47100 47500 49112 10800 8080

