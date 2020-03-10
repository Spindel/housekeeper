# Submit image
FROM registry.gitlab.com/modioab/base-image/fedora-31/python:master
MAINTAINER "D.S. Ljungmark" <ljungmark@modio.se>

ARG URL=unknown
ARG COMMIT=unknown
ARG BRANCH=unknown
ARG HOST=unknown
ARG DATE=unknown

LABEL "se.modio.ci.url"=$URL       \
      "se.modio.ci.branch"=$BRANCH \
      "se.modio.ci.commit"=$COMMIT \
      "se.modio.ci.host"=$HOST     \
      "se.modio.ci.date"=$DATE

# Add our (package)
ADD wheel /tmp/wheel

RUN pip3 install --no-index --find-links=/tmp/wheel housekeeper && \
    echo housekeeper:x:1001:100:housekeeper:/data:/sbin/nologin >> /etc/passwd && \
    rm -rf /tmp/wheel && mkdir /data && chown housekeeper /data

USER 1001
WORKDIR /data
CMD /bin/bash
