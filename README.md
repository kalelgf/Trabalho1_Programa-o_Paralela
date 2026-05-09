# Simulador de Fluxo de Vendas (Pthreads)

Este projeto implementa um protótipo para simular fluxos de trabalhos concorrentes relacionados à venda de itens, focado na exploração de programação paralela e padrões de projeto multithread.

## Requisitos do Sistema
- Sistema Operacional Linux / macOS (ou Windows via WSL/MinGW)
- Compilador `gcc`
- Biblioteca de threads POSIX (`pthread`)
- Ferramenta `make` (opcional, mas recomendada)

## Compilação
Você pode compilar o projeto utilizando o `Makefile` disponibilizado ou diretamente via `gcc`.

**Usando Make:**
```bash
make
```

**Usando GCC diretamente:**
```bash
gcc main.c -o simulador -lpthread -Wall
```

## Execução
Após compilar, execute o binário gerado:
```bash
./simulador
```

## Configurações / Ajustes no Protótipo
A simulação é facilmente customizável. No topo do arquivo `main.c`, há definições em `#define` que alteram o comportamento:
- `TOTAL_PEDIDOS`: Número total de pedidos que os clientes vão gerar.
- `NUM_PRODUTORES`: Quantidade de threads de clientes simultâneos.
- `THREADS_CADASTRO`, `THREADS_FINANCEIRO`, `THREADS_LOGISTICA`: Tamanho das thread pools de cada etapa do processamento.
- `TAM_FILA`: Capacidade máxima de armazenamento simultâneo (buffer) entre as etapas.
- `TAXA_FALHA_CADASTRO`, `TAXA_FALHA_FINANCEIRO`, `TAXA_FALHA_LOGISTICA`: Probabilidades percentuais (0-100) de falha ("reprovação") em uma dada etapa.
- Tempos definidos em microsegundos (`_US` e `_VAR`) podem ser alterados para simular gargalos no fluxo de trabalho.

## Arquitetura
O sistema segue o modelo Produtor-Consumidor operando em Pipeline, com filas com controle de estado Thread-safe garantindo paralelisimo real e seguro livre de Condições de Corrida e Deadlocks. Para mais detalhes, consulte o arquivo `RELATORIO.md`.
