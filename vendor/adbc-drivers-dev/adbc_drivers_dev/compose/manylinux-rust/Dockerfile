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

RUN ulimit -n 1024 && yum install -y wget openssl openssl-devel openssl-static && yum clean all
ENV PATH="/usr/local/rust/bin:${PATH}"

ARG LIBCLANG
ARG RUST
ARG TARGETPLATFORM

RUN mkdir -p /.cargo \
    && chmod 777 /.cargo \
    && git config --global --add safe.directory /source \
    && cp /root/.gitconfig /.gitconfig \
    && chmod 777 /.gitconfig

ADD install_rust.sh /
RUN /install_rust.sh ${RUST} ${TARGETPLATFORM} && rm /install_rust.sh

ADD install_libclang.sh /
RUN /install_libclang.sh ${LIBCLANG} ${TARGETPLATFORM} && rm /install_libclang.sh
ENV LIBCLANG_PATH=/opt/libclang/libclang-${LIBCLANG}.data/platlib/clang/native/libclang.so
