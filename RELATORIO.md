# Relatório Técnico - Protótipo de Sistema de Vendas Multithread

## 1. Arquitetura de Software e Padrões de Projeto Adotados

O projeto foi desenvolvido na linguagem **C** usando a API **POSIX Threads (Pthreads)**, garantindo a execução de paralelismo real com aproveitamento de múltiplos núcleos do processador (sem impedimentos causados por travas globais de interpretador). O fluxo dos pedidos baseia-se em um modelo de **Pipeline** onde blocos independentes processam porções das tarefas simultaneamente.

Para a orquestração segura do multithreading, foram utilizados os seguintes Padrões de Projeto para Programação Paralela:

1. **Pipeline (Arquitetura de Fluxo):**  
   Os dados transitam através de quatro estágios de processamento bem definidos: Clientes (Geração) -> Validação de Cadastro -> Validação Financeira -> Logística e Entrega. As transições ocorrem através de filas intermediárias de forma que nenhum estágio fica impedido de trabalhar caso haja itens a serem processados.

2. **Produtor-Consumidor (Producer-Consumer):**  
   Padrão base da implementação das conexões entre os estágios. A inserção em cada estrutura da `Fila` é realizada por produtores (ex: Clientes gerando compras), e a remoção é realizada pelos consumidores (ex: threads de Cadastro). Mutexes e Variáveis de Condição (`pthread_cond_t`) são usados para suspender as execuções e evitar falhas de concorrência ou desperdício de CPU se uma fila encher (bloqueio do push) ou estiver vazia (bloqueio de pop).

3. **Thread Pool / Trabalhador (Worker / Master-Worker):**  
   Em vez de sobrecarregar o SO gerando uma nova e efêmera Thead a cada pedido (que causaria gargalo de recursos), foram criados _pools_ pré-alocados de trabalho com números fixos de Workers para cada setor. Isso permite estabilidade previsível com threads aguardando e processando os buffers da fila.

4. **Monitor Object:**  
   Visto ativamente no comportamento da infraestrutura de `Fila`. Todas as variáveis relativas ao status da fila (ponteiros de buffer, contadores de lotação e flags de estado do pipeline) estão envoltas (encapsulamento natural) em funções _thread-safe_ dotadas de acesso exclusivo mediante Mutex e condições, atuando na prática como um *Monitor*.

## 2. Casos de Uso e Testes da Solução

Para testar a estabilidade, corretude, e confiabilidade do cenário projetado, as seguintes validações e simulações podem ser acompanhadas durante a execução.

### Cenário A: Execução Padrão Sem Imprevistos
- **Configuração:** `TAXA_FALHA_CADASTRO = 5`, `TAXA_FALHA_FINANCEIRO = 10`, `TAXA_FALHA_LOGISTICA = 5` com `100` pedidos via `3` Produtores simultâneos.
- **Saídas Observadas:**  
   Os logs mostram o intercalamento de geração e processamento:
   ```text
   Cliente 1 gerou pedido 1
   Cliente 2 gerou pedido 2
   Cadastro 1 recebeu pedido 1
   Cadastro 2 recebeu pedido 2
   Financeiro 1 recebeu pedido 1
   ...
   ```
   No **Resumo Final**, observamos estatísticas íntegras: o número `"Total gerados"` bate fielmente com a soma de reprovações mais entregas concluídas, o que evidencia que _nenhum pedido foi perdido em memória nem processado duas vezes_.

### Cenário B: Taxas de Falha Extremas (Comportamento de Descarte)
- **Configuração:** Todas as taxas de falha setadas para `50%`. (Modificação prática no `#define` do código fonte).
- **Saídas Observadas:**  
   A fila final da logística só é acionada por uma quantidade drasticamente menor de instâncias. Isso ratifica o uso do padrão Producer-Consumer com repasse inteligente. Os workers despendem recursos bloqueados esperando aprovações anteriores, demonstrando o efeito gargalo em cadeia natural a modelos reais. Uma mensagem de log indicando `"Financeiro 2 reprovou pedido X"` retira o item imediatamente do contexto do pipeline. Sem corrupção dos arrays e total alinhamento nas somas de descarte.

### Cenário C: Gargalos Extremos / Deadlock Validation
- **Configuração:** `TAM_FILA = 5` (Minúsculo) combinado com total de Pedidos massivos (`5000`) ou introdução de `sleep()` estendidos nos workers.
- **Saídas Observadas:**  
   O programa executa perfeitamente sem o travamento perpétuo das threads clássico em falhas de Pthreads (Deadlock). O sinal de *Broadcast* (`pthread_cond_broadcast`) adotado na função `fila_close()` é disparado ao constatar exaustão sistemática dos _publishers_ da linha matriz, destrancando assertivamente os loopings infinitos *While* de todos os workers e garantindo Encerramento Sustentável (*Graceful Shutdown*).
