# update docker container
docker build -t python-example-solution ../.
docker tag python-example-solution omariani24/python-example-solution:latest
docker push omariani24/python-example-solution:latest
