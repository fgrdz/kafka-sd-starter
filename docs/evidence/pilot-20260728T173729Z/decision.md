# Decisão de carga

A carga foi selecionada após três execuções baseline bem-sucedidas.

Critérios considerados:

- ausência de erros de produção;
- ausência de mensagens confirmadas ausentes;
- ausência de duplicações;
- ausência de violações de ordem;
- consumer lag drenado ao final;
- cluster Kafka estável;
- ausência de saturação contínua da máquina;
- repetibilidade entre execuções.

A configuração foi congelada antes da matriz experimental e será aplicada
igualmente aos Perfis A e B.
