---
name: worker-routing
description: Escolhe o Run, o modelo e o effort antes de despachar um worker. Use ao chamar `orca orchestration worker-start`, ao lançar um subagente pelo Agent tool, ou ao montar os agentes de um Workflow.
---

# Modelo e effort do worker

Vale quando você despacha um worker (Orca `worker-start --model <id> --effort <nível>`, Agent tool, workflow) e precisa escolher modelo e effort. As duas escolhas são independentes:

- **modelo** segue a **ambiguidade**: quanta dúvida existe sobre *qual* solução seguir;
- **effort** segue a quantidade de raciocínio que *esta* execução merece.

Tamanho do diff e número de arquivos são sinais secundários. Trocar uma API em 40 arquivos pode ser Sonnet medium; uma função de 30 linhas que às vezes duplica pagamento pode pedir Opus high.

## Papéis

- **Haiku, scout**: o caminho já está determinado. Busca, localizar símbolo ou uso, resumo de módulo, rename, imports, lint, config simples. Não tem controle de effort.
- **Sonnet, executor padrão** (cerca de 70% das tarefas):
  - **low**: fix óbvio, DTO, validação, campo novo, testes de uma função;
  - **medium**: feature normal, endpoint, refactor seguindo padrões, teste falhando;
  - **high**: debug multi-arquivo, feature que cruza banco, API e front, refactor preservando comportamento.
- **Opus, especialista e escalation**: dúvida real sobre a solução.
  - **high**: arquitetura, boundaries e data model; bug obscuro de causa desconhecida; review de segurança, concorrência ou invariantes; auth, pagamento, migration destrutiva; plano de feature que cruza o sistema;
  - **xhigh**: um worker anterior já falhou, ou há várias hipóteses em aberto;
  - **max**: último recurso, só depois de Opus xhigh falhar.

Planejamento trivial ("campo `phone` com migration, endpoint e formulário") é Sonnet medium, não Opus.

## Overrides fixos

- Busca pura ou mudança mecânica: Haiku.
- Segurança, pagamento ou decisão de arquitetura: Opus high.
- Worker falhou duas vezes na mesma tarefa: Opus xhigh.

## Onde despachar

Toda tarefa do usuário vira task no Run do Orca da sua **frente** (o assunto). O objetivo do Run é o título do card no painel do tablet, que só lê o Orca.

- **Frente nova:** `orca orchestration run-create --objective "<assunto>" --json` antes do primeiro despacho; com o agent manager ligado, rode em seguida `orq gerente ligar --terminal <gerente>`.
- **Frente existente:** despache com `--run <id da frente>`.
- **Tarefa nova nasce como ticket:** `orq ticket novo --titulo "..." --spec-arquivo <f> [--blocked-by NN,NN]` grava `~/.claude/orquestrador-plan/issues/NN-<slug>.md` (o ticket é a única fonte do conteúdo; o spec traz `## Acceptance criteria`) e cria a task no Orca. `orq despachar --ticket <NN> --modelo <m> --effort <e>` sobe o worker nessa task, e `orq ticket fechar <NN> --answer <arquivo|texto>` grava o Answer e completa a task (`docs/design.md`).
- **Despachar:** `orq despachar --run <frente> --titulo "..." --spec-arquivo <f> --modelo <m> --effort <e> [--worktree current|new-top-level --name ... --base-branch ...] [--entrada <e>]`. Ele roda o `worker-start` com modelo e effort, grava o evento `despacho`, liga a entrada (`--entrada`) e devolve `dispatchId`, `taskId` e o comando do waiter pronto (`orca-wait-runs.py`, `espera` no JSON). O título vira a primeira linha do spec e o nome da aba. O coordenador precisa estar ligado ao Run (`run-use --id`).
- **Agent tool:** o subagente não cria task. Antes de lançar, rode `task-create --run <frente> --task-title ... --spec ...` e depois `task-update --status dispatched`. Quando ele voltar, marque `completed` ou `failed`.
- **Agent manager:** os avisos do Orca ("You have N orchestration message", heartbeat incluído) vão só para o terminal ligado ao Run, e terminal sem agente não recebe aviso. Por isso os Runs ficam ligados ao terminal do agent manager, um shell rodando `~/.claude/orq/painel-agent-manager.sh`, e o coordenador só acorda com o que importa (`docs/design.md`).
  - **Ligar:** `orq gerente ligar --terminal <gerente> [--run r]...` no coordenador; soma aos Runs que o gerente já tem, e sem `--run` entra o Run do último `run-create`/`run-use`. O `orq despachar` num Run novo o liga sozinho. `orq gerente desligar [--run r]` devolve um Run ou todos ao coordenador (que segura um só).
  - **Um Run por terminal:** o Orca liga o gerente a um Run por vez; o painel os reveza, e cada comando do `orq` com `--run` (`despachar`, `liberar`, `steer`, `ticket`, o waiter, o hook) religa o gerente ao Run antes, sob a trava. Heartbeat de Run desligado do coordenador não avisa ninguém.
  - **Comando cru do Orca no Run:** prefixe com `env ORCA_TERMINAL_HANDLE=<gerente>` (`check`, `reply`, `worker-start`, `worker-stop`, `task-update`…). O Orca responde `consumer_fenced` se o gerente estiver noutro Run: rode `orq gerente ligar --terminal <gerente> --run <r>` de novo ou espere a volta do painel, que fica no Run do aviso.
  - **O que chega ao coordenador:** o painel confirma os heartbeats a cada 10 s e digita no coordenador `You have N orchestration message. Run orca orchestration check --run <r> --terminal <gerente>.` uma vez por lote com `worker_done`, `question` ou `escalation`, de qualquer Run do gerente. Rode esse `check` como está, processe e confirme com `--ack`. O waiter também acorda com o `worker_done`.
- **Ligação do coordenador:** ele consome o mailbox de um Run só, o último do `run-create` ou do `run-use --id`. Em outro Run, `check` devolve `consumer_fenced`, e `worker-start`, `task-create` e `task-update` falham ou não gravam. Antes de mexer num Run, faça `run-use --id` nele e confira o `ok` do resultado.
- **Run é do coordenador:** o spec do worker manda usar só o Run que recebeu no despacho; o worker não roda `run-create` nem `run-use`. Teste que precise de Run usa `ORQ_HOME` e `ORQ_ORCA` falsos. O `orq` decide o papel de worker pelo preâmbulo de despacho (o primeiro prompt do worker), antes de qualquer Run ligado, então um Run criado por engano não muda o papel, mas suja o painel. Não dá para usar o `worker-list` como sinal: sem `--run` ele só lista o Run ligado ao terminal.
- **Várias frentes ativas:** espere com `~/.claude/scripts/orca-wait-runs.py <run>...`. Ele acompanha o `task-list` de todos os Runs e o mailbox do Run ligado (com o agent manager, o de cada Run dele); nos demais Runs da lista acorda com question, escalation e worker_done lidos no inbox, sem consumir. Para ler e dar ack numa mensagem de outro Run, faça `run-use --id <run>` e volte depois.
- **Heartbeat não acorda o coordenador:** o hook `orq hook prompt` absorve o aviso do Orca quando a caixa do Run ligado só tem heartbeat (`docs/design.md`), e o waiter faz o mesmo. A última fase e a hora de cada despacho rodando aparecem em `Vivos:` no resumo do `orq` e em "Rodando" no painel. Aviso de heartbeat de outro Run é bloqueado do mesmo jeito, lendo o `inbox` e sem confirmar nada: as mensagens ficam na caixa daquele Run até o `run-use --id`. Qualquer outro tipo de mensagem, de qualquer Run, acorda.
- **Ajuste em task rodando:** `orq steer <task> "<texto>" [--run r] [--entrada e]`. Ele acha o dispatch, manda o `send` e registra; precisa do Run do worker ligado por `run-use --id`.
- **Decisão do usuário com worker ativo:** vai por uma página do Lavish no browser do Orca, e o `orq lavish-resposta <arquivo>` grava a resposta (`docs/design.md`). Passe ao comando a saída crua do `lavish-axi poll`, sem extrair o JSON. A página manda `disposicao: "escolha"` (ou `manter`/`trocar`) com a resposta escrita para fechar a decisão; `livre`, `adiar` e `conversar` a deixam aberta. O AskUserQuestion vale só sem despacho ativo: um hook do `orq` recusa a caixa enquanto houver algum, em qualquer Run. Dúvida de worker vai por `orca orchestration ask` ao coordenador (o preâmbulo do Orca já manda), nunca pela caixa.
- **Worker concluído:** `orq liberar <dispatch> [--run r]` confirma o `worker_done` pendente do dispatch, roda o `worker-release` e, se o estado voltar `retained` sem motivo de retenção, roda `orca terminal close` no terminal dele. Terminal que o Orca reteve por motivo (`user_takeover`, `user_requested`, `external_terminal`…) e o do coordenador ficam abertos, com o aviso na saída. Leia a saída depois com `worker-read` (https://www.onorca.dev/docs/cli/orchestration). Se o release voltar `release_pending`, repita depois; `terminal close` à mão não substitui.
- **Quem está vivo:** `orq agentes [--json] [--run r] [--todos]` dá o estado de cada dispatch de todos os Runs: rodando, travado (sem heartbeat há mais de 15 min, com o `orq steer` sugerido), perguntando e entregue (terminal aberto, pronto para o `orq liberar`). O resumo injetado e o painel mostram o mesmo (`docs/design.md`).
- **Espera declarada:** antes de um comando longo que bloqueia (fila de E2E, CI, deploy), o spec manda o worker enviar um heartbeat com `--phase "esperando: <motivo> até HH:MM"` (hora local; sem o `até`, vale 60 min). Até o prazo o dispatch conta como rodando, não travado; vencido, vira travado com "espera vencida". Sem o heartbeat, o worker parado no comando aparece travado aos 15 min.
- **Checkpoint:** o spec do worker manda gravar `orca worktree set --comment` nas transições de fase, no formato de https://www.onorca.dev/docs/cli/worktree-checkpoints (primeira linha é a ação; ler antes com `orca worktree current --json`).
- **Relatório do worker:** o spec manda escrever o relatório (o `reportPath` e o corpo do `worker_done`) com a skill `writing-for-agents`: veredito primeiro, uma fonte por fato, critério de pronto conferível, o resto atrás de ponteiro. Relatório de pesquisa segue a skill `research`.
- **Spec de worker que abre PR:** manda citar só arquivo versionado (`docs/research`, `docs/adr`, `docs/features`) no corpo do PR e subir a pesquisa no mesmo PR quando ele depende dela; `.scratch/` fica de fora.

## Escalation

Suba um degrau por vez quando o worker falhar ou reportar incerteza: Haiku → Sonnet low → medium → high → Opus high → xhigh → max. Peça no spec que o worker devolva `needs_escalation`, com o motivo e o degrau sugerido, em vez de insistir. Assim o Opus não é gasto por precaução.

O spec do retry cita o id da tarefa antiga. Quando o retry termina bem, feche a antiga: `orca orchestration task-update --id <antiga> --status completed --result '{"supersededBy":"<nova>"}'`. Sem isso, ela continua como falha no painel e no `task-list`.

## Calibrar

A meta aproximada é 15% Haiku, 70% Sonnet e 15% Opus, sem virar regra. Quem calibra são os traces: tarefa concluída sem retry, escalations, testes quebrando depois do "pronto" e correções no review.
