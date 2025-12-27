Fonctionnalités v1

1. Input : Lecture Kafka

Connexion à un cluster Kafka (broker address)
Lecture d'un topic source
Configuration du nombre de partitions à lire
Configuration du consumer group
Paramètres de lecture (batch size, timeout, etc.)
2. Processing : Transformations

Chaînage de plusieurs processors dans l'ordre défini
Processors built-in disponibles :

passthrough : Ne fait rien (pour tester)
timestamp_replay : Replay temporel (jour/heure/minute)
field_mapper : Renommer/mapper des champs JSON
filter : Filtrer les messages selon une condition


3. Output : Écriture Kafka

Connexion à un cluster Kafka (même ou différent)
Écriture vers un topic destination
Configuration des partitions cibles
Batching et compression
4. Configuration

Tout paramétré via fichier YAML
Pas de configuration hardcodée dans le code
Validation de la config au démarrage
5. Observabilité basique

Logs structurés (niveau configurable)
Métriques simples affichées périodiquement :

Nombre de messages traités
Débit (msg/s, MB/s)
Erreurs


6. Robustesse

Graceful shutdown (SIGTERM/SIGINT)
Gestion d'erreurs basique (log + continue)
Arrêt automatique si inactivité détectée




## PHASE 1 : CONFIG                        

config.yaml  →  LoadConfig()  →  Config struct (InputConfig,ProcessorConfig,OutputConfig)

## PHASE 2 : CRÉATION DES COMPOSANTS            

Config  →  Pipeline.NewPipeline() 

NewPipeline : 
- Consumer  ← créé depuis InputConfig
- Processors ← créés depuis ProcessorConfig
- Producer  ← créé depuis OutputConfig


## PHASE 3 : ORCHESTRATION                      

Pipeline.Run() → Consumer.Start() → Messages chan

Pour chaque message:
- Message → Processor 1 → Processor 2 → ... → Processor N
- Producer.Send(message)