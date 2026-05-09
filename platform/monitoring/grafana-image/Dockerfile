FROM docker.io/grafana/grafana:12.4.1

USER root

RUN grafana cli plugins install marcusolsson-dynamictext-panel && \
    grafana cli plugins install yesoreyeram-infinity-datasource

USER grafana
