FROM apache/superset:latest

USER root

RUN pip install duckdb==1.0.0
RUN pip install duckdb-engine

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY ./superset-init.sh /superset-init.sh
COPY --chown=superset:superset --chmod=755 ./superset-init.sh /superset-init.sh

USER superset
ENTRYPOINT [ "/superset-init.sh" ]