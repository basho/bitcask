#ifndef __ASYNC_NIF_H__
#define __ASYNC_NIF_H__

#if defined(__cplusplus)
extern "C" {
#endif

/* Redefine this in your NIF implementation before including this file to
   change the thread pool size.  The maximum number of threads might be
   bounded on your OS.  For instance, to allow 1,000,000 threads on a Linux
   system you must do the following before launching the process.
     echo 1000000 > /proc/sys/kernel/threads-max
   and for all UNIX systems there will be ulimit maximums. */
#ifndef ANIF_MAX_WORKERS
#define ANIF_MAX_WORKERS 64
#endif

#ifndef __offsetof
#define __offsetof(st, m) \
     ((size_t) ( (char *)&((st *)0)->m - (char *)0 ))
#endif
#include "queue.h"

struct anif_req_entry {
  ErlNifPid pid;
  ErlNifEnv *env;
  void *args;
  unsigned int assigned_to_worker;
  void (*fn_work)(ErlNifEnv*, ErlNifPid*, void *);
  void (*fn_post)(void *);
  STAILQ_ENTRY(anif_req_entry) entries;
};
STAILQ_HEAD(reqs, anif_req_entry) anif_reqs = STAILQ_HEAD_INITIALIZER(anif_reqs);

struct anif_worker_entry {
  ErlNifTid tid;
  unsigned int worker_num;
  LIST_ENTRY(anif_worker_entry) entries;
};
LIST_HEAD(idle_workers, anif_worker_entry) anif_idle_workers = LIST_HEAD_INITIALIZER(anif_worker);

static volatile unsigned int anif_req_count = 0;
static volatile unsigned int anif_shutdown = 0;
static ErlNifMutex *anif_req_mutex = NULL;
static ErlNifMutex *anif_worker_mutex = NULL;
static ErlNifCond *anif_cnd = NULL;
static struct anif_worker_entry anif_worker_entries[ANIF_MAX_WORKERS];

#define ASYNC_NIF_DECL(name, frame, pre_block, work_block, post_block)  \
  struct name ## _args frame;                                           \
  void fn_work_ ## name (ErlNifEnv *env, ErlNifPid *pid, struct name ## _args *args) \
       work_block                                                       \
  void fn_post_ ## name (struct name ## _args *args) {                  \
    do post_block while(0);                                             \
    enif_free(args);                                                    \
  }                                                                     \
  ERL_NIF_TERM name(ErlNifEnv* env_in, int argc, const ERL_NIF_TERM argv_in[]) { \
    struct name ## _args on_stack_args;                                 \
    struct name ## _args *args = &on_stack_args;                        \
    struct name ## _args *copy_of_args;                                 \
    struct anif_req_entry *r = NULL;                                    \
    ErlNifPid pid;                                                      \
    ErlNifEnv *env = NULL;                                              \
    ERL_NIF_TERM *argv = NULL;                                          \
    if (anif_shutdown) {                                                \
      enif_send(NULL, &pid, env_in,                                     \
                enif_make_tuple2(env_in, enif_make_atom(env_in, "error"), \
                                 enif_make_atom(env_in, "shutdown")));  \
      return enif_make_tuple2(env_in, enif_make_atom(env_in, "error"),  \
                              enif_make_atom(env_in, "shutdown"));      \
    }                                                                   \
    env = enif_alloc_env();                                             \
    if (!env)                                                           \
      return enif_make_tuple2(env_in, enif_make_atom(env_in, "error"),  \
                              enif_make_atom(env_in, "enomem"));        \
    argv = (ERL_NIF_TERM *)enif_alloc(sizeof(ERL_NIF_TERM) * argc);     \
    if (!argv) {                                                        \
      enif_free_env(env);                                               \
      return enif_make_tuple2(env_in, enif_make_atom(env_in, "error"),  \
                              enif_make_atom(env_in, "enomem"));        \
    }                                                                   \
    for (unsigned int i = 0; i < argc; i++) {                           \
      argv[i] = enif_make_copy(env, argv_in[i]);                        \
      if (!argv[i]) {                                                   \
        enif_free(argv);                                                \
        enif_free_env(env);                                             \
        return enif_make_tuple2(env_in, enif_make_atom(env_in, "error"),\
                                enif_make_atom(env_in, "enomem"));      \
      }                                                                 \
    }                                                                   \
    if (!enif_self(env_in, &pid)) {                                     \
      enif_free(argv);                                                  \
      enif_free_env(env);                                               \
      return enif_make_tuple2(env_in, enif_make_atom(env_in, "error"),  \
                              enif_make_atom(env_in, "pid"));           \
    }                                                                   \
    do pre_block while(0);                                              \
    enif_free(argv);                                                    \
    r = (struct anif_req_entry*)enif_alloc(sizeof(struct anif_req_entry)); \
    if (!r) {                                                           \
      fn_post_ ## name (args);                                          \
      enif_free_env(env);                                               \
      return enif_make_tuple2(env_in, enif_make_atom(env_in, "error"),  \
                              enif_make_atom(env_in, "enomem"));        \
    }                                                                   \
    copy_of_args = (struct name ## _args *)enif_alloc(sizeof(struct name ## _args)); \
    if (!copy_of_args) {                                                \
      fn_post_ ## name (args);                                          \
      enif_free_env(env);                                               \
      enif_free(r);                                                     \
      return enif_make_tuple2(env_in, enif_make_atom(env_in, "error"),  \
                              enif_make_atom(env_in, "enomem"));        \
    }                                                                   \
    memcpy(copy_of_args, args, sizeof(struct name ## _args));           \
    memcpy(&(r->pid), &pid, sizeof(ErlNifPid));                         \
    r->args = (void *)copy_of_args;                                     \
    r->env = env;                                                       \
    r->fn_work = (void (*)(ErlNifEnv *, ErlNifPid*, void *))fn_work_ ## name ; \
    r->fn_post = (void (*)(void *))fn_post_ ## name;                    \
    anif_enqueue_req(r);                                                \
    return enif_make_tuple2(env, enif_make_atom(env, "ok"),             \
                            enif_make_int(env, anif_req_count));        \
  }

#define ASYNC_NIF_LOAD() if (anif_init() != 0) return -1;
#define ASYNC_NIF_UNLOAD() anif_unload();
#define ASYNC_NIF_UPGRADE() anif_unload();

#define ASYNC_NIF_PRE_ENV() env_in
#define ASYNC_NIF_PRE_RETURN_CLEANUP() enif_free(argv); enif_free_env(env);
#define ASYNC_NIF_RETURN_BADARG() ASYNC_NIF_PRE_RETURN_CLEANUP(); return enif_make_badarg(env_in);

#define ASYNC_NIF_REPLY(msg) enif_send(NULL, pid, env, msg)

static void anif_enqueue_req(struct anif_req_entry *r)
{
  /* Add the request to the work queue. */
  enif_mutex_lock(anif_req_mutex);
  STAILQ_INSERT_TAIL(&anif_reqs, r, entries);
  anif_req_count++;
  enif_mutex_unlock(anif_req_mutex);
  enif_cond_broadcast(anif_cnd);
}

static void *anif_worker_fn(void *arg)
{
  struct anif_worker_entry *worker = (struct anif_worker_entry *)arg;
  struct anif_req_entry *req = NULL;

  /*
   * Workers are active while there is work on the queue to do and
   * only in the idle list when they are waiting on new work.
   */
  do {
    /* Examine the request queue, are there things to be done? */
    enif_mutex_lock(anif_req_mutex); check_again_for_work:
    if (anif_shutdown) { enif_mutex_unlock(anif_req_mutex); break; }
    if ((req = STAILQ_FIRST(&anif_reqs)) == NULL) {
      /* Queue is empty, join the list of idle workers and wait for work */
      enif_mutex_lock(anif_worker_mutex);
      LIST_INSERT_HEAD(&anif_idle_workers, worker, entries);
      enif_mutex_unlock(anif_worker_mutex);
      enif_cond_wait(anif_cnd, anif_req_mutex);
      goto check_again_for_work;
    } else {
      /* `req` is our work request and we hold the lock. */
      enif_cond_broadcast(anif_cnd);

      /* Take the request off the queue. */
      STAILQ_REMOVE(&anif_reqs, req, anif_req_entry, entries); anif_req_count--;

      /* Now we need to remove this thread from the list of idle threads. */
      enif_mutex_lock(anif_worker_mutex);
      LIST_REMOVE(worker, entries);

      /* Release the locks in reverse order that we acquired them,
         so as not to self-deadlock. */
      enif_mutex_unlock(anif_worker_mutex);
      enif_mutex_unlock(anif_req_mutex);

      /* Finally, let's do the work! :) */
      req->assigned_to_worker = worker->worker_num;
      req->fn_work(req->env, &(req->pid), req->args);
      req->fn_post(req->args);
      enif_free_env(req->env);
      enif_free(req);
    }
  } while(1);
  enif_thread_exit(0);
  return 0;
}

static void anif_unload(void)
{
  /* Signal the worker threads, stop what you're doing and exit. */
  enif_mutex_lock(anif_req_mutex);
  anif_shutdown = 1;
  enif_cond_broadcast(anif_cnd);
  enif_mutex_unlock(anif_req_mutex);

  /* Join for the now exiting worker threads. */
  for (unsigned int i = 0; i < ANIF_MAX_WORKERS; ++i) {
    void *exit_value = 0; /* Ignore this. */
    enif_thread_join(anif_worker_entries[i].tid, &exit_value);
  }

  /* We won't get here until all threads have exited.
     Patch things up, and carry on. */
  enif_mutex_lock(anif_req_mutex);

  /* Worker threads are stopped, now toss anything left in the queue. */
  struct anif_req_entry *e = NULL;
  STAILQ_FOREACH(e, &anif_reqs, entries) {
    STAILQ_REMOVE(&anif_reqs, STAILQ_LAST(&anif_reqs, anif_req_entry, entries),
                  anif_req_entry, entries);
    enif_send(NULL, &(e->pid), e->env,
              enif_make_tuple2(e->env, enif_make_atom(e->env, "error"),
                               enif_make_atom(e->env, "shutdown")));
    e->fn_post(e->args);
    enif_free_env(e->env);
    enif_free(e);
    anif_req_count--;
  }
  enif_mutex_unlock(anif_req_mutex);

  enif_cond_destroy(anif_cnd);
  /* Not strictly necessary. */
  memset(anif_worker_entries, sizeof(struct anif_worker_entry) * ANIF_MAX_WORKERS, 0);

  enif_mutex_destroy(anif_req_mutex); anif_req_mutex = NULL;
  enif_mutex_destroy(anif_worker_mutex); anif_worker_mutex = NULL;
}

static int anif_init(void)
{
  /* Don't init more than once. */
  if (anif_req_mutex) return 0;

  anif_req_mutex = enif_mutex_create(NULL);
  anif_worker_mutex = enif_mutex_create(NULL);
  anif_cnd = enif_cond_create(NULL);

  /* Setup the requests management. */
  anif_req_count = 0;

  /* Setup the thread pool management. */
  enif_mutex_lock(anif_worker_mutex);
  for (unsigned int i = 0; i < ANIF_MAX_WORKERS; i++) {
    anif_worker_entries[i].worker_num = i;
    enif_thread_create(NULL, &anif_worker_entries[i].tid,
                       &anif_worker_fn, (void*)&anif_worker_entries[i], NULL);
  }
  enif_mutex_unlock(anif_worker_mutex);
  return 0;
}

#if defined(__cplusplus)
}
#endif

#endif // __ASYNC_NIF_H__
