FROM eclipse-temurin:25-jre
ARG SIGNAL_CLI_VERSION=0.14.7
ADD https://github.com/AsamK/signal-cli/releases/download/v${SIGNAL_CLI_VERSION}/signal-cli-${SIGNAL_CLI_VERSION}.tar.gz /tmp/signal-cli.tar.gz
RUN tar xzf /tmp/signal-cli.tar.gz -C /opt && rm /tmp/signal-cli.tar.gz \
    && ln -s /opt/signal-cli-${SIGNAL_CLI_VERSION}/bin/signal-cli /usr/local/bin/signal-cli
# account data, keys, and received attachments live under /data
ENTRYPOINT ["signal-cli", "--config", "/data"]
