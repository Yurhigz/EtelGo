## Se connecter au contenru du kafka

```bash
docker exec -it kafka
```

## Produire des messages

```bash
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
```

Ensuite on peut taper des messages qui seront envoyés au topic "test".