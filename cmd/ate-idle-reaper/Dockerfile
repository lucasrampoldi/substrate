FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY ate-idle-reaper /usr/local/bin/ate-idle-reaper
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/ate-idle-reaper"]
