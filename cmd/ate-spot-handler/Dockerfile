FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY ate-spot-handler /usr/local/bin/ate-spot-handler
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/ate-spot-handler"]
