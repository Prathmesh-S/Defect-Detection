FROM flink:1.20.1-scala_2.12
COPY target/DEBS-ACM-Winner.jar /opt/flink/usrlib/
CMD ["flink", "run", "-d", "/opt/flink/usrlib/DEBS-ACM-Winner.jar", "http://challenge2025.debs.org:52923"]

