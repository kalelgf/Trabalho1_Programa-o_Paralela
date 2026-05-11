/* Grupo: Igor Velmud Bandero, Kalel Gomes Freitas
 *
 * Padrões de Projeto de Programação Paralela aplicados:
 * 1. Pipeline: Dividimos o fluxo em estágios (Geração de Pedido -> Validação de Cadastro -> Validação Financeira -> Logística).
 * 2. Produtor-Consumidor: Filas circulares intermediárias interligando os estágios.
 * 3. Thread Pool (Trabalhadores/Workers): Conjuntos predefinidos de threads rodando continuamente para cada estágio.
 * 4. Monitor Object: Estrutura Fila encapsula o buffer, mutex e variáveis de condição para acesso thread-safe.
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <stdarg.h>
#include <stdint.h>

#define TOTAL_PEDIDOS 100
#define NUM_PRODUTORES 3
#define THREADS_CADASTRO 2
#define THREADS_FINANCEIRO 2
#define THREADS_LOGISTICA 2
#define TAM_FILA 20

#define TAXA_FALHA_CADASTRO 5
#define TAXA_FALHA_FINANCEIRO 10
#define TAXA_FALHA_LOGISTICA 5

// Definição de tempos para processamento (base + variação) em microsegundos
#define PRODUTOR_US 20000
#define PRODUTOR_VAR 30000
#define CADASTRO_US 40000
#define CADASTRO_VAR 40000
#define FINANCEIRO_US 50000
#define FINANCEIRO_VAR 50000
#define LOGISTICA_US 60000
#define LOGISTICA_VAR 60000

typedef struct {
  int id;
} Pedido;

/* Padrão 4: Monitor Object
 * Esta estrutura encapsula os dados (buffer, contadores) e os mecanismos
 * de sincronização (mutex e variáveis de condição), garantindo que todas as
 * operações (push/pop) sejam Thread-Safe. */

typedef struct {
  Pedido *buffer;
  int capacidade;
  int inicio;
  int fim;
  int total;
  int fechado;
  pthread_mutex_t mutex;
  pthread_cond_t cond_nao_cheia;
  pthread_cond_t cond_nao_vazia;
} Fila;

typedef struct {
  int total_gerados;
  int aprovados_cadastro;
  int reprovados_cadastro;
  int aprovados_financeiro;
  int reprovados_financeiro;
  int entregues;
  int falhas_logistica;
} Estatisticas;

static Estatisticas stats;
static pthread_mutex_t mutex_stats = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_log = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
  int id;
  Fila *fila;
  int *proximo_id;
  pthread_mutex_t *mutex_id;
} ProdutorArgs;

typedef struct {
  int id;
  Fila *entrada;
  Fila *saida;
} WorkerArgs;

// Função de log thread-safe para evitar mensagens embaralhadas no console
static void log_msg(const char *fmt, ...) {
  va_list args;
  pthread_mutex_lock(&mutex_log);
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
  fflush(stdout);
  pthread_mutex_unlock(&mutex_log);
}

/* Fila circular com produtor-consumidor */
static void fila_init(Fila *f, int capacidade) {
  f->buffer = (Pedido *)malloc(sizeof(Pedido) * capacidade);
  if (!f->buffer) {
    perror("malloc");
    exit(1);
  }
  f->capacidade = capacidade;
  f->inicio = 0;
  f->fim = 0;
  f->total = 0;
  f->fechado = 0;
  pthread_mutex_init(&f->mutex, NULL);
  pthread_cond_init(&f->cond_nao_cheia, NULL);
  pthread_cond_init(&f->cond_nao_vazia, NULL);
}

static void fila_close(Fila *f) {
  pthread_mutex_lock(&f->mutex);
  f->fechado = 1;
  pthread_cond_broadcast(&f->cond_nao_vazia);
  pthread_cond_broadcast(&f->cond_nao_cheia);
  pthread_mutex_unlock(&f->mutex);
}

static void fila_destroy(Fila *f) {
  free(f->buffer);
  pthread_mutex_destroy(&f->mutex);
  pthread_cond_destroy(&f->cond_nao_cheia);
  pthread_cond_destroy(&f->cond_nao_vazia);
}

static int fila_push(Fila *f, Pedido p) {
  pthread_mutex_lock(&f->mutex);
  while (f->total == f->capacidade && !f->fechado) {
    pthread_cond_wait(&f->cond_nao_cheia, &f->mutex);
  }
  if (f->fechado) {
    pthread_mutex_unlock(&f->mutex);
    return 0;
  }
  f->buffer[f->fim] = p;
  f->fim = (f->fim + 1) % f->capacidade;
  f->total++;
  pthread_cond_signal(&f->cond_nao_vazia);
  pthread_mutex_unlock(&f->mutex);
  return 1;
}

static int fila_pop(Fila *f, Pedido *out) {
  pthread_mutex_lock(&f->mutex);
  while (f->total == 0 && !f->fechado) {
    pthread_cond_wait(&f->cond_nao_vazia, &f->mutex);
  }
  if (f->total == 0 && f->fechado) {
    pthread_mutex_unlock(&f->mutex);
    return 0;
  }
  *out = f->buffer[f->inicio];
  f->inicio = (f->inicio + 1) % f->capacidade;
  f->total--;
  pthread_cond_signal(&f->cond_nao_cheia);
  pthread_mutex_unlock(&f->mutex);
  return 1;
}

// Gerador de números aleatórioos
static unsigned int next_rand(unsigned int *seed) {
  *seed = (*seed * 1103515245u) + 12345u;
  return *seed;
}

static int chance_falha(int taxa, unsigned int *seed) {
  int r = (int)(next_rand(seed) % 100u);
  return r < taxa;
}

// Função para introduzir um atraso artificial na execução das threads
static void simula_trabalho(unsigned int *seed, int base_us, int variacao_us) {
  int delta = 0;
  if (variacao_us > 0) {
    delta = (int)(next_rand(seed) % (unsigned int)variacao_us);
  }
  usleep((useconds_t)(base_us + delta));
}

static void *funcao_produtor(void *arg) {
  ProdutorArgs *a = (ProdutorArgs *)arg;
  unsigned int seed = (unsigned int)time(NULL) ^
                      (unsigned int)(a->id * 1103515245u) ^
                      (unsigned int)(uintptr_t)pthread_self();

  while (1) {
    int id;
    pthread_mutex_lock(a->mutex_id);
    if (*(a->proximo_id) > TOTAL_PEDIDOS) {
      pthread_mutex_unlock(a->mutex_id);
      break;
    }
    id = (*a->proximo_id)++;
    pthread_mutex_unlock(a->mutex_id);

    Pedido p;
    p.id = id;

    if (!fila_push(a->fila, p)) {
      break;
    }

    pthread_mutex_lock(&mutex_stats);
    stats.total_gerados++;
    pthread_mutex_unlock(&mutex_stats);

    log_msg("Cliente %d gerou pedido %d\n", a->id, p.id);
    simula_trabalho(&seed, PRODUTOR_US, PRODUTOR_VAR);
  }
  return NULL;
}

/* Worker da etapa de cadastro */
static void *funcao_cadastro(void *arg) {
  WorkerArgs *a = (WorkerArgs *)arg;
  /* Cada worker tem uma semente de randomização única baseada no tempo,
  ID do worker e ID da thread para garantir variação real nas simulações */
  unsigned int seed = (unsigned int)time(NULL) ^
                      (unsigned int)(a->id * 123u) ^
                      (unsigned int)(uintptr_t)pthread_self();

  Pedido p;
  while (fila_pop(a->entrada, &p)) {
    log_msg("Cadastro %d recebeu pedido %d\n", a->id, p.id);
    simula_trabalho(&seed, CADASTRO_US, CADASTRO_VAR);

    if (chance_falha(TAXA_FALHA_CADASTRO, &seed)) {
      pthread_mutex_lock(&mutex_stats);
      stats.reprovados_cadastro++;
      pthread_mutex_unlock(&mutex_stats);

      log_msg("Cadastro %d reprovou pedido %d\n", a->id, p.id);
      continue;
    }

    pthread_mutex_lock(&mutex_stats);
    stats.aprovados_cadastro++;
    pthread_mutex_unlock(&mutex_stats);

    log_msg("Cadastro %d aprovou pedido %d\n", a->id, p.id);

    if (!fila_push(a->saida, p)) {
      break;
    }
  }
  return NULL;
}

/* Worker da etapa financeira */
static void *funcao_financeiro(void *arg) {
  WorkerArgs *a = (WorkerArgs *)arg;
  unsigned int seed = (unsigned int)time(NULL) ^
                      (unsigned int)(a->id * 456u) ^
                      (unsigned int)(uintptr_t)pthread_self();

  Pedido p;
  while (fila_pop(a->entrada, &p)) {
    log_msg("Financeiro %d recebeu pedido %d\n", a->id, p.id);
    simula_trabalho(&seed, FINANCEIRO_US, FINANCEIRO_VAR);

    if (chance_falha(TAXA_FALHA_FINANCEIRO, &seed)) {
      pthread_mutex_lock(&mutex_stats);
      stats.reprovados_financeiro++;
      pthread_mutex_unlock(&mutex_stats);

      log_msg("Financeiro %d reprovou pedido %d\n", a->id, p.id);
      continue;
    }

    pthread_mutex_lock(&mutex_stats);
    stats.aprovados_financeiro++;
    pthread_mutex_unlock(&mutex_stats);

    log_msg("Financeiro %d aprovou pedido %d\n", a->id, p.id);

    if (!fila_push(a->saida, p)) {
      break;
    }
  }
  return NULL;
}

/* Worker da etapa de logistica */
static void *funcao_logistica(void *arg) {
  WorkerArgs *a = (WorkerArgs *)arg;
  unsigned int seed = (unsigned int)time(NULL) ^
                      (unsigned int)(a->id * 789u) ^
                      (unsigned int)(uintptr_t)pthread_self();

  Pedido p;
  while (fila_pop(a->entrada, &p)) {
    log_msg("Logistica %d recebeu pedido %d\n", a->id, p.id);
    simula_trabalho(&seed, LOGISTICA_US, LOGISTICA_VAR);

    if (chance_falha(TAXA_FALHA_LOGISTICA, &seed)) {
      pthread_mutex_lock(&mutex_stats);
      stats.falhas_logistica++;
      pthread_mutex_unlock(&mutex_stats);

      log_msg("Logistica %d falhou no pedido %d\n", a->id, p.id);
      continue;
    }

    pthread_mutex_lock(&mutex_stats);
    stats.entregues++;
    pthread_mutex_unlock(&mutex_stats);

    log_msg("Logistica %d entregou pedido %d\n", a->id, p.id);
  }
  return NULL;
}

int main(void) {
  /* Padrão 1 e 2: Pipeline com Produtor-Consumidor.
   * As filas atuam como buffers intermediários entre os estágios do pipeline. */
  Fila fila_pedidos;
  Fila fila_cadastro_ok;
  Fila fila_financeiro_ok;

  fila_init(&fila_pedidos, TAM_FILA);
  fila_init(&fila_cadastro_ok, TAM_FILA);
  fila_init(&fila_financeiro_ok, TAM_FILA);

  /* Padrão 3: Thread Pool
   * Arrays alocando múltiplas threads (workers) por função para atuarem em paralelo. */
  pthread_t produtores[NUM_PRODUTORES];
  pthread_t cadastro[THREADS_CADASTRO];
  pthread_t financeiro[THREADS_FINANCEIRO];
  pthread_t logistica[THREADS_LOGISTICA];

  ProdutorArgs prod_args[NUM_PRODUTORES];
  WorkerArgs cad_args[THREADS_CADASTRO];
  WorkerArgs fin_args[THREADS_FINANCEIRO];
  WorkerArgs log_args[THREADS_LOGISTICA];

  int proximo_id = 1;
  pthread_mutex_t mutex_id = PTHREAD_MUTEX_INITIALIZER;

  int i;

  for (int i = 0; i < THREADS_CADASTRO; i++) {
    cad_args[i].id = i + 1;
    cad_args[i].entrada = &fila_pedidos;
    cad_args[i].saida = &fila_cadastro_ok;
    pthread_create(&cadastro[i], NULL, funcao_cadastro, &cad_args[i]);
  }

  for (i = 0; i < THREADS_FINANCEIRO; i++) {
    fin_args[i].id = i + 1;
    fin_args[i].entrada = &fila_cadastro_ok;
    fin_args[i].saida = &fila_financeiro_ok;
    pthread_create(&financeiro[i], NULL, funcao_financeiro, &fin_args[i]);
  }

  for (i = 0; i < THREADS_LOGISTICA; i++) {
    log_args[i].id = i + 1;
    log_args[i].entrada = &fila_financeiro_ok;
    log_args[i].saida = NULL;
    pthread_create(&logistica[i], NULL, funcao_logistica, &log_args[i]);
  }

  for (i = 0; i < NUM_PRODUTORES; i++) {
    prod_args[i].id = i + 1;
    prod_args[i].fila = &fila_pedidos;
    prod_args[i].proximo_id = &proximo_id;
    prod_args[i].mutex_id = &mutex_id;
    pthread_create(&produtores[i], NULL, funcao_produtor, &prod_args[i]);
  }

  for (i = 0; i < NUM_PRODUTORES; i++) {
    pthread_join(produtores[i], NULL);
  }
  fila_close(&fila_pedidos);

  for (i = 0; i < THREADS_CADASTRO; i++) {
    pthread_join(cadastro[i], NULL);
  }
  fila_close(&fila_cadastro_ok);

  for (i = 0; i < THREADS_FINANCEIRO; i++) {
    pthread_join(financeiro[i], NULL);
  }
  fila_close(&fila_financeiro_ok);

  for (i = 0; i < THREADS_LOGISTICA; i++) {
    pthread_join(logistica[i], NULL);
  }

  pthread_mutex_destroy(&mutex_id);

  pthread_mutex_lock(&mutex_stats);
  Estatisticas resumo = stats;
  pthread_mutex_unlock(&mutex_stats);

  log_msg("\nResumo final\n");
  log_msg("Total gerados: %d\n", resumo.total_gerados);
  log_msg("Cadastro aprovados: %d\n", resumo.aprovados_cadastro);
  log_msg("Cadastro reprovados: %d\n", resumo.reprovados_cadastro);
  log_msg("Financeiro aprovados: %d\n", resumo.aprovados_financeiro);
  log_msg("Financeiro reprovados: %d\n", resumo.reprovados_financeiro);
  log_msg("Logistica entregues: %d\n", resumo.entregues);
  log_msg("Logistica falhas: %d\n", resumo.falhas_logistica);

  fila_destroy(&fila_pedidos);
  fila_destroy(&fila_cadastro_ok);
  fila_destroy(&fila_financeiro_ok);
  pthread_mutex_destroy(&mutex_stats);
  pthread_mutex_destroy(&mutex_log);

  return 0;
}
