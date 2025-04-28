# Guide to Recreate Visualizations

All visualizations were created via metrics collected on the ACM DEBS Grand Challenge Website. When starting a new benchmark on the platform (via Docker Compose or a Kubernetes script deployment), metrics will be gathered once the benchmark finishes. These include throughput (batches/second), latency, and duration. By aggregating values in an Excel/Google Sheet, graphs were created.  

To reproduce the metrics found in our final presentation, please login to the ACM DEBS Platform and follow  [/flink-solution/docker_compose_solution/README.md]. Please make sure to use the "http://challenge2025.debs.org:52923/" endpoint rather than "http://127.0.0.1:8866" to ensure you use the ACM DEBS Platform rather than local data. 
