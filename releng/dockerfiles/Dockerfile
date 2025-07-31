# Copyright (c) 2023, 2024 Polytechnique de Montréal
#
# All rights reserved. This program and the accompanying materials are made
# available under the terms of the Eclipse Public License 2.0 which
# accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0

FROM maven:3.9-eclipse-temurin-21 as packager

COPY ./ /app/

WORKDIR /app/

RUN mvn clean install -DskipTests -Dskip-rcp -Pfilter-grammar -Pctf-grammar

FROM maven:3.9-eclipse-temurin-21

COPY --from=packager /root/.m2/ /root/.m2/

