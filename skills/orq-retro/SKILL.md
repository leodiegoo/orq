---
name: orq-retro
description: Analisa a rodada do `orq retro` e propõe mudanças no ambiente do orq (checagem, texto, calibragem do worker-routing). Use na retro semanal do orq ou quando o usuário pedir para aprender com as falhas dos workers.
---

# Retro do orq

O coletor (`orq retro`) conta os sinais de falha sem LLM. Esta skill lê a rodada e propõe mudanças no **ambiente**, nunca no código do produto. A fonte da análise é o coletor; os passos de classificar e apresentar são os da skill `retro` (leia-a antes).

## Passos

1. Rode `orq retro --json` (o gatilho semanal soma `--gravar`; sob demanda não grava). A janela padrão é de 7 dias; `--desde AAAA-MM-DD` troca.
2. Para cada sinal com `n > 0`, abra o ponteiro de pelo menos um caso antes de afirmar algo: `sed -n <linha>p ~/.claude/orq/events.jsonl`, a linha do transcrito, o PR. Um sinal só vira proposta com a causa lida no ponteiro.
3. Separe sinal de **ruído**: árvore suja com a mesma contagem em todo `liberado_sujo` do checkout principal é a sujeira que já existia, não do worker; `entrada_sem_tratamento` alto mede o hábito do coordenador, não um bug; steer anterior ao primeiro `steer_fim` do log é de antes da prova de leitura existir. Casos de ruído entram numa linha só.
4. Classifique cada proposta em exatamente uma classe:
   - **checagem**: erro mecânico (padrão fixo, comando proibido, lugar errado). Vira hook, teste ou guarda no orq. Padrão novo de `_retro_viola` entra aqui.
   - **texto**: erro de julgamento. Vira linha no AGENTS.md, no spec padrão do worker ou numa memória. Diga qual arquivo e a linha exata.
   - **calibragem**: o modelo ou o effort errou o bastante. Vira mudança na tabela do `worker-routing`, com o quadro `por_modelo` como prova.
5. Faça a lista curta: no máximo 5 propostas, cada uma com 2 casos ou mais, ou 1 caso que perdeu trabalho ou quebrou o orq. Ordene por gravidade.
6. Escreva a página no Lavish (`.lavish/retro-AAAA-MM-DD.html`, playbook `input`): por proposta, a evidência com ponteiro, a classe, a mudança concreta e a métrica do coletor que deve cair na semana seguinte. Cada proposta leva aprovar, rejeitar ou ajustar.
7. Grave o resumo em `~/.claude/orquestrador-plan/relatorios/retro-AAAA-MM-DD.md` e termine. A rodada só acaba com a página aberta e o relatório escrito.

## Depois do ok

Nada é aplicado sem o ok do usuário na página. Proposta aprovada de **checagem** ou **calibragem** vira ticket (`orq ticket novo`); de **texto**, uma edição no documento, em worker.
