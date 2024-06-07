FROM apache/superset:latest

USER root

RUN pip install duckdb==0.10.3
RUN pip install duckdb-engine==0.12.1

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY ./superset-init.sh /superset-init.sh
COPY --chown=superset:superset --chmod=755 ./superset-init.sh /superset-init.sh

USER superset
ENTRYPOINT [ "/superset-init.sh" ]