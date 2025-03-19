# DEBS'25 Grand Challenge local challenger

This is the local client platform for the DEBS 2025 Grand Challenge.

This implementation demonstrates a Flink-based benchmark for processing image data and analyzing outliers. The project 
uses a custom source to pull data from an API endpoint and processes it using Apache Flink.

## Prerequisites
- Java
- Apache Maven
- IntelliJ
- Local evaluation platform running (refer to [/debs-acm-challenge-example/README](./debs-acm-challenge-example/README.md))

## Set Up
To set up the code to run properly, go to debs-acm-challenge-example/src -> Right Click -> Mark Directory As -> Sources Root

## Running

1. Create a new run configuration with `ClientRef` as the module and pass `"http://127.0.0.1:8866"` as an argument.
2. Run the file

# Contact

Be sure to send us an email if you encounter problems.