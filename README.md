# kafka-sd-starter
Os slides do projeto estão disponíveis no Google Slides:

[Visualizar apresentação](https://docs.google.com/presentation/d/1wHn7KQD5b0yMpqVIYwgzu2dz1XTX5kMu/edit?usp=sharing&ouid=109481235169328712198&rtpof=true&sd=true)

Laboratório acadêmico reproduzível para avaliar tolerância a falhas e
observabilidade de Apache Kafka sobre Kubernetes. O projeto compara dois
perfis experimentais completos durante a remoção abrupta do broker líder e
correlaciona o comportamento das aplicações, do Kafka e da infraestrutura.

O repositório não contém resultados experimentais publicados. O smoke baseline
é um teste de integração da instrumentação e não constitui evidência científica
final.

## Arquitetura

O ambiente local usa:

- Kind para um cluster Kubernetes com um control plane e três workers;
- Strimzi para operar Kafka em modo KRaft;
- três controllers KRaft dedicados e três brokers Kafka dedicados;
- Prometheus e Grafana pelo `kube-prometheus-stack`;
- Kafka Exporter, métricas JMX e `PodMonitor`;
- produtor, consumidor e executor de experimentos escritos em Go;
- arquivos JSONL para eventos de produção, consumo e linha do tempo.

Controllers e brokers usam `KafkaNodePool` separados. Assim, a futura remoção
de um broker líder não remove simultaneamente um controller. O cluster Kafka
continua se chamando `experiment` e o cluster Kind `kafka-experiment`, pois
esses identificadores descrevem componentes técnicos, não o repositório.

As versões estão fixadas em `versions.env`; não são usadas tags `latest`.

## Perfis experimentais

| Perfil | Replicação | `min.insync.replicas` | Confirmação | Idempotência |
|---|---:|---:|---|---|
| A — controle | 1 | 1 | líder | desabilitada |
| B — tolerante a falhas | 3 | 2 | todas as réplicas sincronizadas | habilitada |

Os perfis são unidades completas de comparação. O projeto não atribui
causalmente o resultado a um parâmetro isolado. Partições, carga, tamanho das
mensagens, aplicações, fases e recursos devem permanecer constantes entre os
perfis, salvo justificativa metodológica registrada.

## Pré-requisitos

- Go 1.26;
- Docker com daemon ativo;
- kubectl;
- Kind 0.32.0;
- Helm;
- GNU Make;
- Bash.

Reserve inicialmente 5–7 GiB de RAM, quatro CPUs e cerca de 12 GiB de disco
para Docker/WSL2. O consumo real deve ser verificado antes do piloto.

Confira o ambiente sem criar recursos:

```bash
make doctor
make validate
make test
```

## Instalação local

Os comandos mutáveis são separados para que cada etapa seja explícita:

```bash
make cluster-up
make cluster-status
make monitoring-up
make kafka-up
make images
make load-images
make apps-up
make status
```

`cluster-up` recusa recriar um cluster existente. `monitoring-up` e `kafka-up`
instalam versões fixadas dos charts. As imagens locais
`kafka-sd-starter-producer:dev`, `kafka-sd-starter-consumer:dev` e
`kafka-sd-starter-runner:dev` usam `imagePullPolicy: Never` e precisam ser
carregadas no Kind.

Para remover somente as aplicações:

```bash
make apps-down
```

A destruição do cluster e dos volumes locais exige duas confirmações:

```bash
CONFIRM_CLUSTER_DOWN=kafka-experiment CONFIRM_DELETE_DATA=yes make cluster-down
```

## Comandos principais

```bash
make validate     # valida perfis, versões e YAML
make test         # executa testes unitários
make vet          # análise estática do Go
make build        # compila todos os comandos
make status       # mostra recursos Kubernetes e Kafka
```

O executor também oferece validação, planejamento e modo seguro:

```bash
go run ./cmd/experiment-runner validate --config configs/profile-a.yaml
go run ./cmd/experiment-runner plan --profile B --scenario fault --repetition 1
go run ./cmd/experiment-runner run --profile B --scenario fault --repetition 1 --dry-run
```

## Smoke baseline

Com Kafka pronto, construa e carregue as imagens antes de iniciar o Job:

```bash
make images
make load-images
make smoke-baseline
```

O smoke usa `configs/smoke-baseline.yaml` com o perfil B, 20 mensagens/s,
payload de 256 bytes, três partições, 30 segundos de aquecimento e 90 segundos
de baseline. Um tópico, grupo consumidor e run ID exclusivos evitam colisões.
O produtor é interrompido primeiro; callbacks pendentes são drenados e o
consumidor recebe até 30 segundos para alcançar as confirmações.

O Job usa um PVC temporário para resultados. Os recursos temporários só são
removidos depois de cópia e validação bem-sucedidas; em caso de erro, ficam
preservados para diagnóstico. O target não remove brokers, tópicos existentes,
PVCs do Kafka, namespaces ou o cluster.

Cada execução grava em `data/raw/<run_id>/`:

```text
metadata.json
timeline.jsonl
producer.jsonl
consumer.jsonl
summary.json
integrity.json
prometheus/
kubernetes/
kafka/
logs/
```

`data/raw/` é ignorado pelo Git. Não adicione dados brutos ou resultados
experimentais ao repositório.

## Matriz experimental oficial

A matriz oficial contém 20 execuções: cinco repetições de cada combinação
`A/baseline`, `A/fault`, `B/baseline` e `B/fault`. `REPETITION` identifica
uma execução individual; `REPETITIONS` define quantas repetições de cada
combinação a matriz e o verificador esperam.

Execuções individuais usam os targets abaixo:

```bash
make baseline-a REPETITION=1
make baseline-b REPETITION=1
CONFIRM_DELETE=yes make fault-a REPETITION=1
CONFIRM_DELETE=yes make fault-b REPETITION=1
```

Falhas reais e a matriz exigem `CONFIRM_DELETE=yes` antes de criar qualquer
Job destrutivo. Baselines não recebem token de ServiceAccount. IDs oficiais
começam com `official-`; pilotos e dry-runs nunca contam como evidência oficial.

A matriz é estritamente sequencial, alterna a ordem A/B a cada repetição e
verifica a prontidão do Kafka e dos brokers antes e depois de cada execução.
Ela pode ser retomada: combinações com metadata oficial válida em `data/raw`
são ignoradas, enquanto pilotos, dry-runs, falhas e diretórios incompletos são
desconsiderados. Recursos Kubernetes de uma execução que falha permanecem para
diagnóstico, e uma nova chamada retoma as combinações ainda ausentes.

```bash
CONFIRM_DELETE=yes make matrix REPETITIONS=5 COOLDOWN_SECONDS=30
```

Como a matriz é longa, execute-a dentro de `tmux` na EC2. Faça backup de
`data/raw` antes de iniciar ou retomar e nunca renomeie pilotos para o prefixo
`official-`. Não há execução paralela.

Valide e agregue resultados sem acessar o cluster:

```bash
make matrix-check REPETITIONS=1
make aggregate
```

`matrix-check` exige exatamente as repetições esperadas, valida artefatos,
versões e consistência de configuração. `aggregate` considera somente runs
oficiais válidas e gera `data/processed/runs.csv` e
`data/processed/aggregate.csv`; tempos de recuperação ficam vazios para
baselines. Dados brutos permanecem em `data/raw/<run_id>/`.

## Estrutura do repositório

```text
cmd/                    executáveis de produtor, consumidor, runner e validador
internal/               domínio, clientes, métricas, integridade e persistência
configs/                perfis A, B e smoke baseline
deployments/            Kind, Strimzi, Kafka, monitoramento e aplicações
scripts/                automação operacional reproduzível
docs/                   especificação e plano acadêmico
data/processed/         destino reservado para dados processados
results/                destinos reservados para figuras e tabelas
```

## Limitações atuais

- a injeção de falha remove um pod de broker, não simula falhas de nó, rede ou
  disco; `fault` exige confirmação explícita e também oferece `--dry-run`;
- a coleta de séries do Prometheus e de estados Kubernetes ainda não está
  conectada ao runner;
- o laboratório roda em uma única máquina física e injeta falha de pod, não de
  máquina, rede, disco ou zona;
- recursos são compartilhados, a carga é sintética e o número planejado de
  repetições é pequeno;
- os perfis alteram múltiplos parâmetros simultaneamente;
- resultados não devem ser generalizados diretamente para produção.

Consulte [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md) para as perguntas de
pesquisa, métricas e critérios de recuperação, e
[docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) para as próximas
fases.

## Referências

- Kreps, J.; Narkhede, N.; Rao, J. *Kafka: a Distributed Messaging System for
  Log Processing*. NetDB, 2011.
- Apache Kafka. *Documentation*.
- Strimzi. *Apache Kafka on Kubernetes documentation*.
- Kubernetes SIGs. *Kind documentation*.
- Prometheus Authors. *Prometheus documentation*.
