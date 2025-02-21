# DEBS Design Document

## Members

1. Prathmesh Sonawane
2. Owen Mariani
3. Tanish Bhowmick

## Problem Statement 

L-PFB is a manufacturing method where layers of fine metal powder are melted on top of each other to create objects. The problem is that in some cases, the object can have high levels of porosity (or empty space) in some areas, leading the object to be defected. Though objects can be assessed after the L-PFB process finishes, a lot of time, energy, and material could have been saved by detecting defects and shutting down the process earlier. Thus, there is a need to implement real-time monitoring systems that can detect defects as they are made, resulting in the L-PFB process stopping as defects are found. Such a solution would inhibit a system from continuing the L-PBF process when a defect is already likely, leading to saved time and resources that can be spent to create a new, un-defected object. 

### Goal:
We hope to create a solution that halts the L-PFB manufacturing process in real time if defects are found, saving valuable time and resources. Finding such a solution will save manufacturers using the L-PFB technique time, energy, and materials, decreasing costs that may then be sold to consumers for a lower price. Thus, both manufacturers and consumers of L-PFB products can benefit from such a solution. 


## Given Data
Each optical tomography image contains, for each point **P = (x, y)**, the temperature **T(P)** at that point, measured layer by layer (**z**). In our input stream, we are given tiles for each layer, layer by layer, where each time contains points, each with a T(P).

## Desired Output
For each input **tile** received from the stream, the solution should return:
1. The **number of saturated points**.
2. The **centroid** (**x, y** coordinates) and **size** (number of points) of the **top 10 largest clusters**.

## Proposed Solution/Processing Pipeline

The following "processing pipeline" is given to us by the DEBS 2025 website. 

    1. Saturation analysis: Within each tile, detect all points that surpass a threshold value of 65000.

    2. Windowing: For each tile, keep a window of the last three layers.

    3. Outlier analysis: Within each tile window, for each point P of the most recent layer:
        - Compute its local temperature deviation D as the absolute difference between:
            - The mean temperature T(P) of its close neighbors (Manhattan distance 0 ≤ d ≤ 2 across 3 layers).
            - The mean temperature of its outer neighbors (Manhattan distance 2 < d ≤ 4).
        - A point is classified as an outlier if D > 5000.

    4. Outlier clustering: Using the outliers computed for the last received layer, find clusters of nearby outliers using DBScan, with the Euclidean distance between points as the distance metric.

By finding the number of saturated points and our cluster centers/sizes for points that are outliers, defects can be detected. Our understanding is that thermal outliers are directly linked to defects. Thus, by finding thermal outliers, we can find when defects are likely to occur. 

Though DEBS gives us a sample solution written in Python, our group has decided to use Kafka and Flink to funnel and manipulate our streaming data. These tools have been taught in class and have the necessary functionality to aggregate/manipulate streaming data with a variety of operators. 


### Technical Tools for Each Step: 

**Note: The DEBS sample solution was not working until 2/18 when the DEBS organizers pushed a fix. This limited our testing capabilities for the design document.**

1. To detect all points that surpass a threshold for each tile, we can use the filter operator and filter out data points that don't meet our threshold. After, we can simply key-by the tileID and run a count function. 

2. Flink/Kafka automatically supports the usage of windows. For this step, we could use sliding windows after key-by using tileID of size three layers and step one layer. 

3. Using our window of size 3, we can then compute the average temperature for each point in each tile for the latest layer, finding the local and outer averages. Then, we can simply see if the deviation is large enough, and use a filter function to pass those points on. 

4. Lastly, with all the deviated points, we can keyby tileID, plot those points using DBScan, find our top 10 clusters, and feed those to our sink. 


## Expectations 
By creating our solution based on their criteria for finding defects, we hope to intake and process our data quicker and more effectively, leading to stream outputs that define deviated clusters that are more accurate and precise. The grader can then use these more precise cluster centers and sizes (our stream output) to better predict if a defect has occurred. 

In terms of alternatives, out team has currently stuck with the steps given to us by the DEBS team since those steps directly define what data we need to output to detect defects later on. 


## Experimental Plan
### Data & Simulation:
- Use the temperature dataset given to us by the DEBS team. Through the online portal, we can also introduce network delays and node crashes. These will be used to test the fault tolerance of our application.

### Performance & Fault Testing + Testing Tools:
- Measure throughput and latency using a docker container that tests your local solution files. 
- Once local tests pass, deploy our solution via the DEBS competition’s pipeline, which benchmarks Kubernetes clusters and simulates network and pod failures.

### Monitoring & Metrics & Measurements:
- Collect system metrics (CPU, memory, throughput, latency) using Kafka/Flink built-in tools and any metrics returned by the DEBS website after a submission
- Validate detection accuracy by comparing outputs (saturated point counts, cluster centroids/sizes) against ground-truth data.

### Deployment Pipeline:
- Utilize the DEBS deployment pipeline to upload our Kubernetes clusters, enabling automated benchmarking and fault injection tests (network and pod failures).
- Run iterative experiments through this pipeline to fine-tune performance, scalability, and resilience under realistic conditions.

This plan will confirm our hypotheses by ensuring the system accurately detects defects while maintaining performance and scalability under realistic operational conditions.
## Success Indicators
By the conclusion of the semester, this project will be able to effectively find clusters of deviated points, thus better feeding into a model to predict defects, when deployed as a task in the Kubernetes cluster. Our markers of progress will be the following: 

#### Progress Markers 
For what "performantly" and "unperformantly" mean please refer to the **Metrics** section below. In general "performantly means that the solution has both a competitive latency and throughput in relation to the other competitors and is able to replicate those scores repeatedly.

1. Basic Solution that accomplishes the task locally (unperformantly)
The system is setup such that it can take in images and output confirmation that it has received each image. The solution is able to correctly:
    * Identify high saturation points in images (check against sample solution)
    * Cluster those outliers (check against sample solution)
2. Basic Solution that accomplishes the task unperformantly in the deployed environment
3. Solution that outperforms the template solution in the deployed environment
4. Solutions that perform increasingly well on the leaderboard and have better metrics than previous runs in the deployed environment
5. Solution is cleaned up and scores well in the following metrics

#### Metrics 
Our measures for success are directly tied to the metrics by which our project will be evaluated: **throughput and latency**. However, even if these two metrics are the most important, we aim to also keep in mind the following characteristics as nice-to-haves:
* **Horizontal scalability**: the ability to enhance the performance of the solution by adding more hardware via efficient paralellization
* **Operational Reliability/Resilience**: the solution is able to overcome network and pod failures gracefully 
* **Accessibility of source code**: there is good documentation and guiding tutorials for how to best use the solution
* integration with standard (tools/protocols)
* **Portability/Maintainability**: The solution can be used in multiple environments and on multiple platforms. It is also well structures/commented for long term maintainability

## Task Assignment

Specific Tasks to be completed:

- Get the DEBS data stream working using Kafka/Flink (Set up as a Data Faucet)
- Basic Architecture: Calculating Saturation Points + Outlier Detection & Clustering
- Implementing Basic Architecture
- Testing Basic Architecture Locally + on the DEBS platform
- Test for Fault Tolerance via the DEBS Benchmark (failures):
- Further fine tuning (Ex. #cores or other adjustments), Validation, and Testing
- Repeat Steps for better architectures 


Tasks involving the big-picture, like thinking of new architectures, will be done collaboratively. However, smaller tasks like implementing parts of the architecture (Ex. counting saturated points OR finding outliers) can be done independently and in parallel. Furthermore, testing and fine-tuning will be done independently and in parallel. 

Some tasks are dependent on each other:
- Data ingestion using Kafka/Flink must be completed first. 
- We must think of architectures before implementing them, further later testing them
- Validation runs iteratively across modules.

Although we all have similar strengths and weaknesses in event-driven systems, we’ll distribute tasks based on interest and maintain regular coordination to ensure smooth and efficient progress throughout the project.