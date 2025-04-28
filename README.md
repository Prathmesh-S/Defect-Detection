# DEBS ACM Grand Challenge Submission - BU1

## File Organization

`/debs-acm-challenge-example` - the example given by the organizing body

- follow [/debs-acm-challenge-example/README](./debs-acm-challenge-example/README.md) for how to run the docker container with the data

`/flink-solution` - our Apache Flink Application that handles the same task with better performance and guarantees
- follow [/debs-acm-challenge-example/RUN_CLIENT](./debs-acm-challenge-example/RUN_CLIENT.md) for how to run our client locally
- follow [/flink-solution/deployment-scripts/README.md](./flink-solution/deployment-scripts/README.md) for how to spin up a local Kubernetes cluster using *Kind* and *Kubectl*
- follow [/flink-solution/docker_compose_solution/README.md](./flink-solution/docker_compose_solution/README.md) for how to spin up a local cluster using *Docker Compose*

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
