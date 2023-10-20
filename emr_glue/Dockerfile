# Environment Dockerfile for EMR
FROM --platform=linux/amd64 amazonlinux:2 AS base

RUN yum install -y python3

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt /tmp/
RUN python3 -m pip install --upgrade pip
RUN pip install --requirement /tmp/requirements.txt
RUN pip install pytz --upgrade
COPY .. /tmp/


RUN mkdir /output && venv-pack -o /output/environment.tar.gz

FROM scratch AS export
COPY --from=base /output/environment.tar.gz /