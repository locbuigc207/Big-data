# How to run
Full guide with issues provide later
## 1. Create namespace and set default
```bash
kubectl create ns debezium
kubectl config set-context --current --namespace=debezium
```

## 2. Apply all YAML file 
```bash
kubectl apply -f ./debezium 
kubectl apply -f ./stream_processing
```

## 3. Create data for testing
Get the postgres pod ```<pod-name>``` first:  
```bash
kubectl get pods
```

```bash
kubectl exec -it  <pod-name> -- psql -U cdc -d cdc
```

## 4. Make your update in database and monitor the captured change in kafka topic

```bash
kubectl exec -it  <pod-name> -- psql -U cdc -d cdc
```
then make some change (CREATE, INSERT, UPDATE,...)

See the captured change:
```bash
kubectl run -n debezium -it --rm --image=quay.io/strimzi/kafka:0.40.0-kafka-3.7.0 \
  kafka-client --restart=Never -- \
  bin/kafka-console-consumer.sh \
    --bootstrap-server debezium-cluster-kafka-bootstrap:9092 \
    --topic postgres.public.customers \
    --from-beginning
```
