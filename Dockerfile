FROM alpine:3.12
MAINTAINER TechOps "tech-ops-team@snowplowanalytics.com"

ADD build/output/linux/cli/stream-replicator /root/

CMD ["/root/stream-replicator"]
