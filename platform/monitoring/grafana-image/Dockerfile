FROM docker.io/grafana/grafana:12.4.1

USER root

RUN mkdir -p /usr/share/grafana/custom-plugins && \
    grafana cli --pluginsDir /usr/share/grafana/custom-plugins \
      plugins install marcusolsson-dynamictext-panel && \
    grafana cli --pluginsDir /usr/share/grafana/custom-plugins \
      plugins install yesoreyeram-infinity-datasource && \
    chmod -R 755 /usr/share/grafana/custom-plugins

USER grafana
