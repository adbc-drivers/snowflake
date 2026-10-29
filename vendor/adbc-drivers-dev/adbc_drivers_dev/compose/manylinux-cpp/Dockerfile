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

RUN ulimit -n 1024 && \
    yum install -y \
      autoconf \
      automake \
      binutils \
      cmake \
      curl \
      gcc \
      gcc-c++ \
      kernel-devel \
      make \
      ninja-build \
      patch \
      patchutils \
      perl \
      perl-IPC-Cmd \
      pkgconfig \
      tar \
      unzip \
      wget \
      zip \
      && \
    yum clean all
ENV PATH="/usr/local/go/bin:${PATH}"

ARG VCPKG_REVISION
ARG VCPKG_SHALLOW_SINCE
ARG TARGETPLATFORM

ENV VCPKG_ROOT=/opt/vcpkg

RUN git clone --shallow-since="${VCPKG_SHALLOW_SINCE}" https://github.com/microsoft/vcpkg "${VCPKG_ROOT}" \
    && cd "${VCPKG_ROOT}" \
    && git checkout --detach "${VCPKG_REVISION}" \
    && test "$(git rev-parse HEAD)" = "${VCPKG_REVISION}" \
    && ./bootstrap-vcpkg.sh -disableMetrics \
    && chmod -R a+rwX "${VCPKG_ROOT}"
ENV PATH="${VCPKG_ROOT}:${PATH}"

RUN git config --global --add safe.directory /source \
    && cp /root/.gitconfig /.gitconfig \
    && chmod 777 /.gitconfig

COPY only-export-adbc.ld /only-export-adbc.ld
