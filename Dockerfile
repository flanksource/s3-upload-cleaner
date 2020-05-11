# build stage
FROM golang:alpine AS build-env
RUN apk --no-cache add build-base git bzr mercurial gcc
ADD . /src
RUN cd /src && go build -o s3-upload-cleaner

# final stage
FROM alpine
WORKDIR /app
COPY --from=build-env /src/s3-upload-cleaner /app/
ENTRYPOINT ./s3-upload-cleaner