# Guide to Recreate Visualizations

All visualizations were created via metrics collected on the ACM DEBS Grand Challenge Website. When starting a new benchmark on the platform (via Docker Compose or a Kubernetes script deployment), metrics will be gathered once the benchmark finishes. These include throughput (batches/second), latency, and duration. By aggregating values in an Excel/Google Sheet, graphs were created.  

To reproduce the metrics found in our final presentation, please first log in to the ACM DEBS Platform. 

Once logged in, navigate to flink-solution/src/main/java/solution/ClientRef.java. Mark the Java solution as root.

In the flink-solution directory (where pom.xml is located)  run:
mvn clean package

Next In your IDE, go to ClientRef.java. If prompted, set up a Java SDK. Now, back on ClientRef.java, click the green arrow next to the main method to modify the run configuration. If this does not show up, you may need to manually create a run configuration and set the module/mainClass = CLientRef.
On the modify run configuration page, provide the argument: "http://challenge2025.debs.org:52923"
Add the option to "Add dependencies with 'provided' scope to classpath" by clicking "Modify Options"

Then, you can simply run the file by clicking on the green triangle. 

In the logs of IntelliJ or your IDE, you will see batches come in and get processed. Once the benchmark finishes, you will be able to see a new benchmark on the ACM DEBS website when logged-in. Clicking on details of a certain benchmark, you can view its throughput, latency, and duration. 