FROM grafana/grafana:latest


# Enable anonymous access and set the default role to Viewer
ENV GF_AUTH_ANONYMOUS_ENABLED=true
ENV GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer

# Optionally, override additional configuration settings if needed.
# For example, disable login hints or set a custom root URL:
# ENV GF_SERVER_ROOT_URL=%(protocol)s://%(domain)s:%(http_port)s/


# Copy the dashboard provisioning file into Grafana’s provisioning folder.
COPY dashboard-provisioning.yaml /etc/grafana/provisioning/dashboards/

# Create the dashboards directory as specified in the provisioning file and copy the dashboard JSON.
RUN mkdir -p /var/lib/grafana/dashboards
COPY simple_time_dash.json /var/lib/grafana/dashboards/

EXPOSE 3000

CMD [ "/run.sh" ]
