FROM alpine:3.12.0
RUN apk add --no-cache sqlite-dev mariadb-connector-c libpq

ADD https://teleport-releases.s3.us-east-2.amazonaws.com/v0.0.1-alpha.1/teleport_0.0.1-alpha.1.linux-x86_64.tar.gz /tmp/
RUN tar xzvf /tmp/teleport_0.0.1-alpha.1.linux-x86_64.tar.gz teleport_0.0.1-alpha.1.linux-x86_64/teleport
RUN mv /teleport_0.0.1-alpha.1.linux-x86_64/teleport /teleport
RUN rmdir /teleport_0.0.1-alpha.1.linux-x86_64/
RUN rm /tmp/teleport_0.0.1-alpha.1.linux-x86_64.tar.gz

RUN mkdir /pad

ENV PADPATH "/pad"
ENTRYPOINT ["/teleport"]
CMD ["version"]
