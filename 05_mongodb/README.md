## Mongo DB Practice

The practice works in Docker using Docker Compose.

You can run it either Locally (as we saw in the docker lecture) or with [Github Codespace](https://30daysof.github.io/data-science-day/week-2/1-codespaces/)

### Locally

- Pull the latest version of this repository
- cd into the mongodb folder
- run docker compose
  - you can use Visualstudio Code
  - you can use a dockerised installation of Jupyter
  - in the compose there is also a mongo express client container to visualise the content of the database. Accessible locally on port 8081
  
### CodeSpace

Open Codespace as indicated in the images below (use the main branch).
And run docker in the codespace. From here on is the same as locally.

![codespaces](../codespaces-howto.png)

![visualstudio](codespaces-visualstudio.png)

### Using Visual Studio

``` docker compose up -d ```

### Launchign with Jupyter Lab (in browser)

``` docker compose -f docker-compose-notebook.yml up -d```


## Legacy

Here there are the instruction in case you would like to install mongo locally without docker.

### Windows users:

- We install it on windows using the MSI version (https://www.mongodb.com/try/download/community?tck=docs_server), cutomize the installation to "c:/mongodb"
- Add the "data/db"  and "logs"" dirs into the installation directory which you already customized.
- From the CMD **"As administrator"**, configure the logs and databases directories, and start the mongoDB service:
    -  from the "bin "directory run the following command>>> <code>mongod --directorydb --dpath c:\mongodb\data\db --logpath c:\mongodb\log\mongo.log --logappend --rest --install </code>

- Now we can run the mongodb service 
    - net start mongodb
- Putting your mongoDBHome/bin to the enviroment variables Paths:
    - so you can run the Shell of MongoDb using the command '>mongo'

### Linux users (ubuntu):
- Follow the instructions in this [tutourial](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/) to install MongoDB 4.4 Community Edition on LTS (long-term support) releases of Ubuntu Linux using the apt package manager.
- Genrally Speaking, you can also follow this [link](https://docs.mongodb.com/manual/administration/install-on-linux/) to install MongoDB Community Edition for supported Linux systems. 


### Docker users

Simply clone the repository and run

```bash
docker-compose up
```

The following docker compose file will build the notebook container which includes all the required dependencies.
Services are also exposed to the host network so you can connect to the via localhost.

[Open Jupyter](http://127.0.0.1:8888/)


```yaml
version: "3"

services:
  mongo:
    image: mongo
    ports:
        - 27017:27017
        - 27018:27018
    restart: always
    environment:
      MONGO_INITDB_ROOT_USERNAME: 
      MONGO_INITDB_ROOT_PASSWORD: 
  notebook:
    build: notebook/
    ports:
      - 8888:8888
    volumes:
       - ./:/home/jovyan/work/data
    environment:
      - GRANT_SUDO=yes
```     


#### Good to know (MongoDB in the Cloud ([Mongo-Atlas](https://docs.atlas.mongodb.com/getting-started/)))

- If you are using MongoDB in the Cloud (Atlas), you will need to:
    - [Create an Atlas Account and Cluster](https://docs.atlas.mongodb.com/getting-started/)
    - [Set Up Connectivity to Atlas](https://docs.mongodb.com/guides/cloud/connectionstring/)
