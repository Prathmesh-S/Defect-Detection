FROM flink:1.20.1-scala_2.12
# Copy JAR to correct directory
COPY target/DEBS-ACM-Solution.jar /opt/flink/lib/
# Reset to default entrypoint
ENTRYPOINT ["/docker-entrypoint.sh"]
