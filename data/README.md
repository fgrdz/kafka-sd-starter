# Dados experimentais

## Matriz oficial

A matriz final possui 20 execuções válidas, com cinco repetições em cada combinação: Perfil A/baseline, Perfil A/fault, Perfil B/baseline e Perfil B/fault. Smoke tests, pilotos e execuções excluídas não fazem parte da matriz.

## Organização

- `raw/<run_id>/`: artefatos primários de cada execução.
- `excluded/`: execuções duplicadas ou retiradas da análise com justificativa.
- `processed/runs.csv`: uma linha por execução oficial válida.
- `processed/aggregate.csv`: estatísticas por perfil, cenário e métrica.
- `processed/analysis-summary.txt`: cobertura, estatísticas e taxas.
- `processed/results-summary.md`: interpretação concisa e limitações.
- `processed/raw-checksums.sha256`: hashes SHA-256 de todos os arquivos das 20 execuções oficiais.
- `processed/charts/`: figuras finais em PNG (300 dpi) e PDF.

## Reprodução

Com os dados brutos restaurados em `data/raw`, execute na raiz:

```bash
make matrix-check REPETITIONS=5
make aggregate
python3 scripts/analyze_results.py > data/processed/analysis-summary.txt
python3 scripts/generate_charts.py
```

`matrix_results.py` valida e consolida as runs oficiais; `analyze_results.py` gera o resumo; `generate_charts.py` gera as figuras a partir de `runs.csv`. Esses scripts preservam os critérios da análise final.

Verifique o conjunto bruto com:

```bash
sha256sum --check data/processed/raw-checksums.sha256
```

Os checksums confirmam, arquivo a arquivo, que o conjunto restaurado é idêntico ao usado na consolidação, sem publicar os artefatos primários.

## Política de versionamento

`data/raw/` não é versionado por volume, por conter artefatos operacionais detalhados e por não ser necessário para consultar os resultados finais. `data/excluded/` também permanece fora do Git. Ambos devem ser preservados em armazenamento separado e controlado. CSVs, resumos, checksums e gráficos em `data/processed/` devem ser versionados.

## Limitações de observabilidade

CPU, memória, rede e séries do Prometheus não foram coletadas de forma válida na matriz final. Esses sinais não são apresentados nem inferidos; as conclusões se limitam às métricas efetivamente registradas e aos marcos de recuperação disponíveis.
