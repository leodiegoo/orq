# 07: demo slice 7: agent manager

Status: resolved
Blocked by: 06
Run: run_demo00000001
Task: task_demo00000007

## What to build

Entregar `orq agentes`, o alerta de travado e a limpeza automática com `orq liberar`.

### Entregar

1. **`orq agentes [--json]`:** o estado de cada dispatch.
2. **Alerta de travado:** dispatch sem heartbeat há mais de 15 min aparece no resumo.

### Testes

Testes com `ORQ_ORCA` falso.

## Acceptance criteria

- [x] `orq agentes` lista todos os dispatches
- [x] o alerta de travado aparece no resumo

## Comments

Nada pendente.

## Answer

Entregue no commit abc1234.
