FROM apache/airflow:3.1.8

USER root

RUN apt-get update && apt-get install -y curl tar gzip && \
    curl -LO https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable/openshift-client-linux.tar.gz && \
    tar -xzf openshift-client-linux.tar.gz && \
    mv oc kubectl /usr/local/bin/ && \
    chmod +x /usr/local/bin/oc /usr/local/bin/kubectl && \
    rm -f openshift-client-linux.tar.gz README.md && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
