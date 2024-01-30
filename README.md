# Steps to test 

### Kafka Setup
Make sure you have the docker running and then point the docker-compose file attached and execute the command below
  ```cd go-kafka-redis-async ```
  ```docker-compose up -d ```

### Redis Setup
   ```docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest```
  
### Install the following dependencies 

  ``` go get "github.com/google/uuid ```
  
  ``` go get github.com/redis/go-redis/v9 ```
  
  ``` go get github.com/Shopify/sarama ```

### Run the server
``` go run main.go```


#### Posting: You can use postman/curl to post any string in the body and the Url below
 ```localhost:8080/api/post```

#### Getting: You can get all the records from the redis using 
```localhost:8080/api/get```
