# Plano incremental de implementação

A implementação deve avançar em fases, mantendo o repositório compilável ao final de cada uma.

## Fase 0 — inspeção e decisões

- verificar ferramentas instaladas;
- detectar versão do Go;
- verificar Docker, kubectl, Kind e Helm sem instalar silenciosamente;
- registrar decisões técnicas no README;
- criar `go.mod` usando a versão local disponível;
- não executar operações destrutivas.

## Fase 1 — fundação do repositório

Criar:

- estrutura de diretórios;
- configuração YAML e validação;
- tipos de domínio;
- formato de mensagem;
- escritor JSONL thread-safe;
- logger estruturado;
- Makefile;
- `.gitignore`;
- README inicial;
- testes unitários da fundação.

Critério:

```bash
go test ./...
go vet ./...
go build ./...
```

## Fase 2 — produtor

Implementar:

- cliente `franz-go` configurável por perfil;
- particionamento explícito e determinístico;
- sequência global e por partição;
- limitador de taxa;
- callbacks de confirmação;
- métricas Prometheus;
- logs JSONL de tentativas e resultados;
- encerramento gracioso;
- testes sem broker real.

## Fase 3 — consumidor e integridade

Implementar:

- grupo consumidor;
- commit explícito de offsets;
- decodificação e validação;
- latência ponta a ponta;
- rastreamento de duplicações;
- verificação de ordem por partição;
- exportação de IDs consumidos;
- métricas Prometheus;
- testes unitários de integridade.

## Fase 4 — infraestrutura local

Criar manifestos e scripts para:

- Kind;
- namespaces;
- Strimzi;
- Kafka com três brokers;
- armazenamento persistente local;
- Prometheus e Grafana;
- PodMonitor/ServiceMonitor;
- deployments de produtor e consumidor;
- tópicos dos perfis A e B.

Não fixar versões arbitrárias sem documentar a fonte e a compatibilidade.
Não executar destruição automática.

## Fase 5 — executor experimental

Implementar CLI com subcomandos ou flags para:

- validar ambiente;
- criar metadados da execução;
- criar tópico único;
- aguardar estabilidade inicial;
- iniciar aplicações;
- marcar aquecimento e linha de base;
- descobrir broker líder;
- mapear broker para pod;
- executar `--dry-run`;
- remover o pod quando autorizado;
- registrar timeline;
- acompanhar marcos de recuperação;
- encerrar por recuperação ou timeout;
- exportar dados.

Integrações reais devem ficar atrás de interfaces testáveis.

## Fase 6 — detectores de recuperação

Implementar funções puras ou componentes testáveis para:

- infraestrutura;
- cluster;
- aplicações;
- desempenho por janela móvel de 60 segundos.

Adicionar testes com séries temporais sintéticas em `testdata/`.

## Fase 7 — piloto e automação de repetições

Criar comandos para:

- uma execução manual;
- uma execução completa;
- matriz de perfis e cenários;
- cinco repetições;
- ordem de execução configurável e seed registrada;
- limpeza segura entre execuções.

Não gerar nem preencher resultados fictícios.

## Fase 8 — análise e documentação

Adicionar:

- transformação de dados brutos para CSV;
- cálculo das métricas por execução;
- tabelas agregadas;
- documentação para gráficos;
- checklist da demonstração;
- seção de limitações e ameaças à validade.

A geração de gráficos pode ser implementada depois, em ferramenta separada, sem alterar o núcleo em Go.
