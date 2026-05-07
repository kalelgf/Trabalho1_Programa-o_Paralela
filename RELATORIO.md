# Esboco do relatorio tecnico

## 1. Arquitetura de software adotada
- Fluxo em pipeline: clientes -> cadastro -> financeiro -> logistica.
- Filas circulares entre etapas, sincronizadas com mutex e cond.
- Pools de threads fixos por etapa.

## 2. Justificativa para os padroes
- Producer-Consumer: clientes geram pedidos e cadastro consome a fila inicial com exclusao mutua.
- Pipeline: cada etapa processa e encaminha pedidos para a proxima fase.
- Thread Pool: numero fixo de workers por etapa para controlar paralelismo.

## 3. Casos de uso/teste sugeridos
- Taxas 0/0/0: todos os pedidos devem chegar a logistica.
- Taxas altas (ex. 50/50/50): observar reducao de aprovacao.
- Aumentar pedidos e filas para verificar estabilidade e ausencia de deadlock.
