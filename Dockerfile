# Container image for beanstalkd-pi. See documentation.txt section 9
# ("Running in a container") for usage.
#
# beanstalkd-pi links github.com/mattn/go-sqlite3 (cgo), so the binary is
# built with the golang image's own gcc against glibc and runs on
# distroless/base (glibc, no shell, nonroot user) rather than scratch or
# distroless/static. The builder stage runs on the target platform, so a
# multi-arch `docker buildx build --platform linux/amd64,linux/arm64`
# builds each arch natively (or under QEMU) instead of needing the zig
# cross toolchain the release workflow uses.

FROM golang:1.27.3-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

RUN CGO_ENABLED=1 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
      -o /out/beanstalkd-pi . \
 && mkdir -p /out/data

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/beanstalkd-pi /usr/local/bin/beanstalkd-pi
# Owned by the image's nonroot user so a fresh named volume mounted here
# inherits writable ownership.
COPY --from=build --chown=nonroot:nonroot /out/data /data

VOLUME /data

# 11300: beanstalk protocol. 11301: observability HTTP (/metrics, /healthz).
EXPOSE 11300 11301

ENTRYPOINT ["/usr/local/bin/beanstalkd-pi"]
CMD ["-addr", ":11300", "-http-addr", ":11301", "-db", "/data/jobs.db"]
