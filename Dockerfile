FROM alpine:3.12.0
RUN apk add --no-cache sqlite-dev mariadb-connector-c libpq
COPY ./teleport teleport

RUN mkdir /pad

ENV PADPATH "/pad"
ENTRYPOINT ["/teleport"]
CMD ["version"]
