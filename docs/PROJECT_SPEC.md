# Especificação do projeto

## Título

Avaliação experimental da tolerância a falhas e da observabilidade do Apache Kafka sobre Kubernetes.

## Objetivo geral

Avaliar experimentalmente como a falha de um broker líder afeta:

- disponibilidade;
- latência;
- vazão;
- consumer lag;
- integridade do fluxo de mensagens;
- tempos de recuperação.

A observação deve correlacionar três níveis:

1. infraestrutura Kubernetes;
2. estado interno do Kafka;
3. comportamento das aplicações produtoras e consumidoras.

## Perguntas de pesquisa

### PP1

Qual é o impacto da falha de um broker líder sobre disponibilidade, latência, vazão, consumer lag e integridade do fluxo de mensagens nos dois perfis avaliados?

### PP2

Quanto tempo o sistema leva para se recuperar, separando:

- recriação do pod;
- recuperação do cluster Kafka;
- retomada das aplicações;
- retorno do desempenho à linha de base?

### PP3

Quais métricas da aplicação, Kafka e Kubernetes permitem detectar a falha, acompanhar suas etapas e identificar a recuperação completa?

## Hipóteses

### H1

A falha do broker líder provocará aumento temporário da latência, redução da vazão e crescimento do consumer lag. O impacto sobre disponibilidade e integridade deverá ser menor no perfil tolerante a falhas.

### H2

O pod poderá retornar a `Running` e `Ready` antes da recuperação completa do Kafka e das aplicações.

### H3

Nenhuma métrica isolada será suficiente; será necessária correlação entre métricas dos três níveis.

## Ambiente

- Kubernetes local com Kind;
- Apache Kafka gerenciado pelo Strimzi;
- três brokers Kafka;
- KRaft, de acordo com a versão selecionada do Strimzi/Kafka;
- Prometheus;
- Grafana;
- métricas JMX do Kafka;
- aplicações em Go;
- executor de experimentos em Go.

Todas as versões devem ser registradas.

## Perfis

### Perfil A — controle

- fator de replicação: 1;
- `min.insync.replicas`: 1;
- produtor confirmando pelo líder;
- idempotência desabilitada.

A falha do broker líder torna temporariamente indisponíveis as partições hospedadas exclusivamente nele.

### Perfil B — tolerante a falhas

- fator de replicação: 3;
- `min.insync.replicas`: 2;
- `acks=all` ou configuração semanticamente equivalente no cliente Go;
- idempotência habilitada.

Os perfis são comparados como configurações completas.

## Cenários

Para cada perfil:

1. cenário sem falha, usado como linha de base/controle;
2. cenário com remoção abrupta do broker líder.

Planejamento mínimo:

- cinco repetições independentes por combinação;
- resultados tratados de forma descritiva e exploratória;
- sem conclusões estatísticas fortes com amostra pequena.

## Carga

Antes da coleta principal deve existir um piloto para escolher uma carga estável que não sature a máquina.

Variáveis controladas:

- tamanho da mensagem;
- taxa de produção;
- quantidade de partições;
- quantidade de produtores e consumidores;
- grupo consumidor;
- política de chaveamento/particionamento;
- duração das fases;
- limites de CPU e memória.

## Formato mínimo da mensagem

```json
{
  "run_id": "b-fault-01",
  "message_id": "b-fault-01-p2-00000421",
  "global_sequence": 1258,
  "partition": 2,
  "partition_sequence": 421,
  "produced_at_unix_ns": 1785072400123456789,
  "payload": "..."
}
```

## Procedimento experimental

1. iniciar o cluster;
2. confirmar brokers, líderes e réplicas estáveis;
3. iniciar métricas;
4. iniciar produtor e consumidor;
5. executar aquecimento;
6. registrar linha de base;
7. identificar o broker líder de ao menos uma partição;
8. registrar o instante da falha;
9. remover abruptamente o pod;
10. manter as aplicações ativas;
11. observar eleição, recriação e sincronização;
12. encerrar ao atingir recuperação ou timeout;
13. exportar métricas, logs, eventos e registros de mensagens.

## Métricas

### Aplicação

- taxa de tentativa de produção;
- taxa de confirmação;
- falhas de produção;
- latência entre envio e confirmação;
- taxa de consumo;
- latência ponta a ponta;
- consumer lag;
- mensagens confirmadas ausentes;
- duplicações;
- violações de ordem por partição.

### Kafka

- partições sem líder;
- réplicas fora de sincronia;
- mudanças de liderança;
- taxa e latência de requisições;
- estado de líderes e ISR.

### Kubernetes

- pod `Running`;
- pod `Ready`;
- reinicializações;
- tempo de recriação;
- eventos;
- CPU;
- memória.

## Critérios de recuperação

O instante zero é a remoção do pod.

### Infraestrutura

O novo pod do broker alcança `Running` e `Ready`.

### Cluster

- não existem partições sem líder;
- réplicas fora de sincronia retornam a zero;
- ISR retorna ao tamanho esperado para o perfil.

### Aplicações

- produtor volta a receber confirmações;
- consumidor volta a processar sem interrupção prolongada.

O intervalo mínimo de confirmação de estabilidade da aplicação deve ser configurável e documentado.

### Desempenho

Durante 60 segundos contínuos:

- vazão >= 90% da mediana da linha de base;
- latência p95 <= 110% da referência;
- consumer lag <= p95 da linha de base.

Os limiares podem ser ajustados após o piloto apenas se a mudança for justificada e aplicada igualmente a todas as execuções.

## Análise

Para cada execução, calcular no mínimo:

- disponibilidade de produção;
- intervalo de indisponibilidade;
- vazão antes, durante e depois;
- latência p50 e p95;
- pico e duração do consumer lag;
- tempos dos quatro níveis de recuperação;
- mensagens confirmadas ausentes;
- duplicações;
- violações de ordem.

Agregações finais:

- valores individuais;
- mediana;
- percentis adequados;
- mínimo e máximo;
- séries temporais alinhadas no instante da falha.

## Limitações que devem aparecer na documentação

- cluster local em uma única máquina física;
- falha de processo/pod, não falha independente de máquina, rede, disco ou zona;
- recursos compartilhados;
- carga sintética;
- número reduzido de repetições;
- perfis alteram múltiplos parâmetros simultaneamente;
- resultados não devem ser generalizados diretamente para produção.

## Entregáveis

- relatório técnico;
- repositório reproduzível;
- apresentação oral;
- gráficos e quadros comparativos;
- demonstração gravada do evento de falha e recuperação.
