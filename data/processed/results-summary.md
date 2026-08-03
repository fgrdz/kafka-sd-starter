# Síntese dos resultados

A matriz oficial contém 20 execuções válidas, cinco para cada combinação de perfil e cenário. Em todas, `attempted = acknowledged + failed` e `consumed = acknowledged`; não há `run_id` duplicado nem execução de `data/excluded/` nos CSVs.

Nos baselines, ambos os perfis reconheceram e consumiram todas as mensagens. Sob remoção do broker líder, o Perfil A apresentou falhas explícitas de produção nas cinco repetições, enquanto o Perfil B manteve zero falhas explícitas, zero mensagens reconhecidas ausentes, zero duplicações e lag final zero. A principal conclusão é a continuidade do serviço e a integridade observada no Perfil B durante a falha testada.

`sequence_gaps` registra ocorrência de lacuna na sequência observada; não mede diretamente mensagens perdidas. Houve uma ocorrência em cada execução A/fault e nenhuma em B/fault. Como `acknowledged_missing` foi zero, o indicador não deve ser descrito como contagem de mensagens perdidas.

Os tempos de recuperação de infraestrutura foram praticamente equivalentes entre os perfis (medianas de 43,036 s para A e 43,027 s para B). A diferença não sustenta afirmar que o Perfil B recuperou a infraestrutura mais rapidamente. Os gráficos e a interpretação priorizam continuidade e integridade, não throughput máximo nem superioridade de recuperação.

CPU, memória, rede e séries do Prometheus não foram coletadas de forma válida na matriz final e não integram as conclusões.
