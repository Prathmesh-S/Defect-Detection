FROM flink:1.20.1-scala_2.12

# Copy the packaged JAR into the Flink container
COPY target/DEBS-ACM-Solution.jar /opt/flink/usrlib/

# Set the entrypoint for Flink
ENTRYPOINT ["flink", "run", "-c", "main.java.solution.ClientRef", "/opt/flink/usrlib/DEBS-ACM-Solution.jar"]
#CMD ["flink", "run", "-d", "/opt/flink/usrlib/DEBS-ACM-Winner.jar", "http://challenge2025.debs.org:52923"]

# Default arguments (optional) - this allows you to override them at runtime
CMD ["http://challenge2025.debs.org:52923"]
