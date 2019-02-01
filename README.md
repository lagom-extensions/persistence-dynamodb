#### Lagom DynamoDB persistence with read side journal streaming

##### Lagom contribution
Main goal to continue evolution as part of Lagom framework and supported by community

##### Approximate DynamoDB cost
This is extremely high in payment. 
We have done a check with 4200RPS and got ~ 4300$/month DynamoDB price.
![prices](./docs/DynamoDBPrices.jpg)
So just a suggestion for AWS, don't use DynamoDB for akka persistence, but can be used as read side db.

##### Library dependencies used
- Alpakka DynamoDB
- Scanamo
- Akka Persistence DynamoDB
- Amazon DynamoDB Streams Kinesis Adapter

##### AWS infrastructure emulation
- localstack
- ensure AWS credential variables are set as bash profile(sbt envVars just don't applied in Intellij seldom test run)
```bash
export AWS_ACCESS_KEY_ID=dummy-access-key
export AWS_SECRET_KEY=dummy-secret-key
export AWS_REGION=eu-west-1
```
- Open terminal and run all infrastructure under Docker. On macos TMPDIR=/private$TMPDIR docker-compose up
```bash
docker-compose up
```

#### Development integration notes
##### 1. Separate journal table per micro service, this will reduce redundant events traffic
We stream events from DynamoDB with AWS KCL, there is no way to provide filtering by tag, 
all events from specified table streaming ARN will comes to us as from one Stream Source 
and be filtered and routed to appropriate by tag Sink(per PersistentEntity eventsByTag Source).

##### 2. AWS KCL initial stream starts from oldest available data record(Affects only cases when late enable read side db)
With AWS KCL we can't be sure that initial start stream will process all events, 
as events on table will starts from oldest available data record(they work on internal DynamoDB streams that defaults to store last 7 days).
Generally this should not affect most work as by DynamoDB checkpoints used for next start stream position. 

##### 3. On very high load project ensure correct DynamoDB checkpoint interval configuration
On application restart DynamoDB data stream will start from last checkpoint, this can results in extra data being fetched. 
From our side there is internal sequence based offset filtering to prevent processing already processed events.

##### 4. Optimization on event processing
Usually in CQRS approach there are not a lot of sense of raw event and you have to ask for some state. 
So if application journal events will be with some state, and this state is actually what is being replicated to read side table.
Then it will be very efficient to allow persist only one max by sequence event from chunk of events that comes from DynamoDB.

##### 5. Small duplication in configuration
This comes because of inconsistency between 
akka-persistence-dynamodb and alpakka-dynamodb

#### Suggested next enhancements:
It seems that this can be simplified if go with polling/sleeping/notification approach, something similar to what exists in Cassandra Journal

#### Code structure and quality
A lot of code comes in a similar approach to all read side processors integration, so most left without any refactoring