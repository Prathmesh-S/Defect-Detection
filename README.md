# DEBS ACM Grand Challenge Submission - BU1

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

## Our Solution/Pipeline
The following "processing pipeline" is given to us by the DEBS 2025 website.

    1. Saturation analysis: Within each tile, detect all points that surpass a threshold value of 65000.

    2. Windowing: For each tile, keep a window of the last three layers.

    3. Outlier analysis: Within each tile window, for each point P of the most recent layer:
        - Compute its local temperature deviation D as the absolute difference between:
            - The mean temperature T(P) of its close neighbors (Manhattan distance 0 ≤ d ≤ 2 across 3 layers).
            - The mean temperature of its outer neighbors (Manhattan distance 2 < d ≤ 4).
        - A point is classified as an outlier if D > 5000.

    4. Outlier clustering: Using the outliers computed for the last received layer, 
        find clusters of nearby outliers using DBScan, with the Euclidean distance between points as the distance metric.



## File Organization/Running our Project

`/debs-acm-challenge-example` - the example given by the organizing body

- follow [/debs-acm-challenge-example/README](./debs-acm-challenge-example/README.md) for how to run the naive Python solution via the docker container with the data

`/flink-solution` - our Apache Flink Application that handles the same task with better performance and guarantees
- follow [/flink-solution/deployment-scripts/README.md](./flink-solution/deployment-scripts/README.md) for how to spin up a local Kubernetes cluster using *Kind* and *Kubectl*
- follow [/flink-solution/docker_compose_solution/README.md](./flink-solution/docker_compose_solution/README.md) for how to spin up a local cluster using *Docker Compose* (using online evaluator, thus not needing any data stored locally)

`/tests` - correctness tests we created that compare the outputs of sample solution to our solution

`/visualizations` - scripts used to create the plots and charts used to display performance and the problem

`/documentation` - design documents and meeting notes from our planning meetings

## Data + Local Evaluator Download

The data needed to run the example project is too large to store on GitHub. Please download it from this link:

[Data for testing here](https://drive.google.com/file/d/1a7eS1tb2SsohfmRDsnlKs9IFKzQpgN7z/view?usp=sharing)

[Local Evaluator here](https://drive.google.com/drive/u/0/folders/1ywnzGsSpGZzng7zVbkEhzPAX0JeKW2O4)

## Team Resources

Check out the Shared Drive [here](https://drive.google.com/drive/u/0/folders/1ywnzGsSpGZzng7zVbkEhzPAX0JeKW2O4)

And our [Running Document](https://docs.google.com/document/d/17led0yd2KA_CpYaShUNldbBG8bbIVOmMDc99vvl4vBY/edit?tab=t.0)

## API Documentation

Login to evaluate and stuff [here](https://challenge2025.debs.org/docs/)

Check slides provided for [API endpoint specification](https://docs.google.com/presentation/d/1rBOWwbdFKXaHX5j_ChI0MbPOuiF2LD250ssT8hePQhY/edit#slide=id.g33465615dac_0_87)

Copy and paste the content of **[/debs-acm-challenge-example/openapi.yml](/debs-acm-challenge-example/openapi.yml)** into `https://editor.swagger.io/`

## Contributors

<a href="https://contrib.rocks">
  <img src="https://contrib.rocks/image?repo=owenm-26/dems-acm-challenge-bu" />
</a>
