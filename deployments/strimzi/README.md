# Instalação do Strimzi

O target `make kafka-up` instala o chart oficial `strimzi-kafka-operator` na
versão registrada em `versions.env`, aguarda o operador e aplica o cluster KRaft.
Ele não atualiza silenciosamente uma instalação existente com versão diferente.

Nenhuma instalação é executada por `doctor`, `validate` ou pelos testes locais.
