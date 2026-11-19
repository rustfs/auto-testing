FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 xfsprogs util-linux ca-certificates && rm -rf /var/lib/apt/lists/*
COPY rustfs /usr/bin/rustfs
COPY candidate.json /opt/node_replacement_candidate.json
COPY node_replacement_node.py /opt/node_replacement_node.py
ENTRYPOINT ["python3", "/opt/node_replacement_node.py", "serve"]
