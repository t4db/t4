FROM clojure:temurin-24-lein-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      openssh-client \
      gnuplot \
      libjna-java \
      git \
    && rm -rf /var/lib/apt/lists/*

# Jepsen connects to DB nodes using this private key.
# Generate with: make keys  (see jepsen/Makefile)
COPY id_ed25519 /root/.ssh/id_ed25519
RUN chmod 700 /root/.ssh && chmod 600 /root/.ssh/id_ed25519 \
 && printf 'Host n*\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n' \
    >> /etc/ssh/ssh_config

WORKDIR /jepsen
