# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG MANYLINUX
FROM quay.io/pypa/${MANYLINUX}:latest

RUN ulimit -n 1024 && yum install -y wget && yum clean all
ENV PATH="/usr/local/go/bin:${PATH}"

ARG GO
ARG TARGETPLATFORM

RUN mkdir -p /.cache/go-build \
    && chmod 777 /.cache/go-build \
    && git config --global --add safe.directory /source \
    && cp /root/.gitconfig /.gitconfig \
    && chmod 777 /.gitconfig

ADD install_go.sh /
RUN /install_go.sh ${GO} ${TARGETPLATFORM} && rm /install_go.sh

COPY only-export-adbc.ld /only-export-adbc.ld
