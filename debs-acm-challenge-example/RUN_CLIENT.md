# DEBS'25 Grand Challenge local challenger

This is the local client platform for the DEBS 2025 Grand Challenge. This implementation demonstrates a Flink-based benchmark for processing image data and analyzing outliers. The project 
uses a custom source to pull data from an API endpoint and processes it using Apache Flink.

## Prerequisites
- Java Development Kit (JDK) 11 or higher
- Apache Maven
- IntelliJ IDEA (or your favorite IDE)
- Docker (for running the evaluation container)
- Data Files
  - The actual evaluator and input data (TIFF images) is required to run the benchmark. The data is hosted on Google Drive. [Download the data here](https://drive.google.com/drive/folders/16QyqxShJ4dejY0uExaZUyYWN4F0amO3h?usp=sharing).

## Set Up
1. Clone the repository
  ```
  git clone https://github.com/CS-551/team-debs-2-debs-dubs.git
  ```
2. Make sure the evaluator and input data is downloaded. Unpack them and place the folders in the `debs-acm-challenge-example` directory if they are not already there.
3. In IntelliJ, navigate to `debs-acm-challenge-example/src` and mark it as the "Sources Root" to ensure that the project compiles correctly.
4. **Download Java Packages via Maven:** Go to the root directory (where pom.xml is located) and run:
  ```
  mvn clean package
  ```
5. To start up the local evaluator platform, run the following in a terminal from the root directory:
  ```
  ./debs-acm-challenge-example/run.sh
  ```
  > Note: If this does not work you may have to run `chmod +x debs-acm-challenge-example/run.sh`


## Running the Application

1. **Create a Run Configuration:**
In your IDE (e.g., IntelliJ), create a new run configuration:
    - Set the module to ClientRef
    - Provide the argument: "http://127.0.0.1:8866"
    - Add the option to "Add dependencies with 'provided' scope to classpath"
2. Run the file

## Input and Output Data
**Input Data:**
The input consists of TIFF image files with ZIP compression. Note that these files are currently using dummy data (for example, a 100x100 matrix with the top-left quadrant set to a threshold value).

Output Data:
The output is printed to the console, showing:

- The layer number, tile ID, and batch ID for each processed batch.
- The count of saturated image pixels (values above a given threshold).
- The centroids and sizes of outlier clusters we found. 

> Note: If you want to print the values for any items that pass through any part of out processing pipeline, simply create a datastreamsink with a print method attached to it. 

> Note: Because the data is dummy for now, the output will reflect the hard-coded values defined in the processing logic. Once we find a solution to the image decompression, we should be able to use the same unchanged pipeline to get our correct results. 

# Contact

Be sure to send us an email if you encounter problems.
