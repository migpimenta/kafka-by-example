FROM confluentinc/cp-kafka:7.5.0
USER root
COPY kafka-init.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/kafka-init.sh
USER appuser

ENTRYPOINT ["/usr/local/bin/kafka-init.sh"]
