/*
 *
 * async_nif: An async thread-pool layer for Erlang's NIF API
 *
 * Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 * Author: Gregory Burd <greg@basho.com> <greg@burd.me>
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

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
#ifndef ASYNC_NIF_MAX_WORKERS
#define ASYNC_NIF_MAX_WORKERS 64
#endif

#ifndef __offsetof
#define __offsetof(st, m) \
     ((size_t) ( (char *)&((st *)0)->m - (char *)0 ))
#endif
#include "queue.h"

struct async_nif_req_entry {
  ERL_NIF_TERM pid, ref, *argv;
  ErlNifEnv *env;
  void *args;
  void *priv_data;
  void (*fn_work)(ErlNifEnv*, ErlNifEnv*, ERL_NIF_TERM, void *, ErlNifPid*, void *);
  void (*fn_post)(void *);
  STAILQ_ENTRY(async_nif_req_entry) entries;
};
STAILQ_HEAD(reqs, async_nif_req_entry) async_nif_reqs = STAILQ_HEAD_INITIALIZER(async_nif_reqs);

struct async_nif_worker_entry {
  ErlNifTid tid;
  ErlNifEnv *env;
  LIST_ENTRY(async_nif_worker_entry) entries;
};
LIST_HEAD(idle_workers, async_nif_worker_entry) async_nif_idle_workers = LIST_HEAD_INITIALIZER(async_nif_worker);

static volatile unsigned int async_nif_req_count = 0;
static volatile unsigned int async_nif_shutdown = 0;
static ErlNifMutex *async_nif_req_mutex = NULL;
static ErlNifMutex *async_nif_worker_mutex = NULL;
static ErlNifCond *async_nif_cnd = NULL;
static struct async_nif_worker_entry async_nif_worker_entries[ASYNC_NIF_MAX_WORKERS];


#define ASYNC_NIF_DECL(decl, frame, pre_block, work_block, post_block)  \
  struct decl ## _args frame;                                           \
  static void fn_work_ ## decl (ErlNifEnv *env, ErlNifEnv *env_in, ERL_NIF_TERM ref_in, void *priv_data, ErlNifPid *pid, struct decl ## _args *args) { \
    ERL_NIF_TERM ref = enif_make_copy(env, ref_in);                     \
    do work_block while(0);                                             \
  }                                                                     \
  static void fn_post_ ## decl (struct decl ## _args *args) {           \
    do post_block while(0);                                             \
  }                                                                     \
  static ERL_NIF_TERM decl(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) { \
    struct decl ## _args on_stack_args;                                 \
    struct decl ## _args *args = &on_stack_args;                        \
    struct decl ## _args *copy_of_args;                                 \
    struct async_nif_req_entry *req = NULL;                             \
    ErlNifPid pid_in;                                                   \
    ERL_NIF_TERM ref = argv[0];                                         \
    if (async_nif_shutdown)                                             \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "shutdown"));         \
    argc--; argv++; /* argv[0] is used internally for selective recv. */ \
    do pre_block while(0);                                              \
    enif_self(env, &pid_in);                                            \
    req = (struct async_nif_req_entry*)enif_alloc(sizeof(struct async_nif_req_entry)); \
    if (!req) {                                                         \
      fn_post_ ## decl (args);                                          \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),  \
                              enif_make_atom(env, "enomem"));        \
    }                                                                   \
    copy_of_args = (struct decl ## _args *)enif_alloc(sizeof(struct decl ## _args)); \
    if (!copy_of_args) {                                                \
      fn_post_ ## decl (args);                                          \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),  \
                              enif_make_atom(env, "enomem"));        \
    }                                                                   \
    memcpy(copy_of_args, args, sizeof(struct decl ## _args));           \
    req->ref = ref;                                                     \
    req->pid = enif_make_pid(env, &pid_in);                             \
    req->args = (void*)copy_of_args;                                    \
    req->priv_data = enif_priv_data(env);                            \
    req->fn_work = (void (*)(ErlNifEnv *, ErlNifEnv *, ERL_NIF_TERM, void*, ErlNifPid*, void *))fn_work_ ## decl ; \
    req->fn_post = (void (*)(void *))fn_post_ ## decl;                  \
    async_nif_enqueue_req(req);                                         \
    return enif_make_tuple2(env, enif_make_atom(env, "ok"),             \
                            enif_make_tuple2(env, enif_make_atom(env, "enqueued"), \
                                             enif_make_int(env, async_nif_req_count))); \
  }

#define ASYNC_NIF_LOAD() if (async_nif_init() != 0) return -1;
#define ASYNC_NIF_UNLOAD() async_nif_unload();
#define ASYNC_NIF_UPGRADE() async_nif_unload();

#define ASYNC_NIF_RETURN_BADARG() return enif_make_badarg(env);

#ifndef PULSE
#define ASYNC_NIF_REPLY(msg) enif_send(NULL, pid, env, enif_make_tuple2(env, ref, msg))
#else
#define ASYNC_NIF_REPLY(msg) PULSE_SEND(NULL, pid, env, enif_make_tuple2(env, ref, msg))
#endif

static void async_nif_enqueue_req(struct async_nif_req_entry *r)
{
  /* Add the request to the work queue. */
  enif_mutex_lock(async_nif_req_mutex);
  STAILQ_INSERT_TAIL(&async_nif_reqs, r, entries);
  async_nif_req_count++;
  enif_mutex_unlock(async_nif_req_mutex);
  enif_cond_broadcast(async_nif_cnd);
}

static void *async_nif_worker_fn(void *arg)
{
  struct async_nif_worker_entry *worker = (struct async_nif_worker_entry *)arg;
  struct async_nif_req_entry *req = NULL;

  /*
   * Workers are active while there is work on the queue to do and
   * only in the idle list when they are waiting on new work.
   */
  for(;;) {
    /* Examine the request queue, are there things to be done? */
    enif_mutex_lock(async_nif_req_mutex);
    enif_mutex_lock(async_nif_worker_mutex);
    LIST_INSERT_HEAD(&async_nif_idle_workers, worker, entries);
    enif_mutex_unlock(async_nif_worker_mutex);
    check_again_for_work:
    if (async_nif_shutdown) { enif_mutex_unlock(async_nif_req_mutex); break; }
    if ((req = STAILQ_FIRST(&async_nif_reqs)) == NULL) {
      /* Queue is empty, join the list of idle workers and wait for work */
      enif_cond_wait(async_nif_cnd, async_nif_req_mutex);
      goto check_again_for_work;
    } else {
      /* `req` is our work request and we hold the lock. */
      enif_cond_broadcast(async_nif_cnd);

      /* Take the request off the queue. */
      STAILQ_REMOVE(&async_nif_reqs, req, async_nif_req_entry, entries); async_nif_req_count--;

      /* Now we need to remove this thread from the list of idle threads. */
      enif_mutex_lock(async_nif_worker_mutex);
      LIST_REMOVE(worker, entries);

      /* Release the locks in reverse order that we acquired them,
         so as not to self-deadlock. */
      enif_mutex_unlock(async_nif_worker_mutex);
      enif_mutex_unlock(async_nif_req_mutex);

      /* Finally, let's do the work! :) */
      ErlNifPid pid;
      enif_get_local_pid(req->env, req->pid, &pid);
      req->fn_work(worker->env, req->env, req->ref, req->priv_data, &pid, req->args);
      req->fn_post(req->args);
      enif_clear_env(worker->env);
      enif_free(req->args);
      enif_free(req);
    }
  }
  enif_thread_exit(0);
  return 0;
}

static void async_nif_unload(void)
{
  unsigned int i;

  /* Signal the worker threads, stop what you're doing and exit. */
  enif_mutex_lock(async_nif_req_mutex);
  async_nif_shutdown = 1;
  enif_cond_broadcast(async_nif_cnd);
  enif_mutex_unlock(async_nif_req_mutex);

  /* Join for the now exiting worker threads. */
  for (i = 0; i < ASYNC_NIF_MAX_WORKERS; ++i) {
    void *exit_value = 0; /* Ignore this. */
    enif_thread_join(async_nif_worker_entries[i].tid, &exit_value);
  }

  /* We won't get here until all threads have exited.
     Patch things up, and carry on. */
  enif_mutex_lock(async_nif_req_mutex);

  /* Worker threads are stopped, now toss anything left in the queue. */
  ErlNifEnv *env = enif_alloc_env();
  struct async_nif_req_entry *req = NULL;
  STAILQ_FOREACH(req, &async_nif_reqs, entries) {
    STAILQ_REMOVE(&async_nif_reqs, STAILQ_LAST(&async_nif_reqs, async_nif_req_entry, entries),
                  async_nif_req_entry, entries);
    ErlNifPid pid;
    enif_get_local_pid(env, req->pid, &pid);
#ifdef PULSE
    PULSE_SEND(NULL, &pid, env,
              enif_make_tuple2(env, enif_make_atom(env, "error"),
                               enif_make_atom(env, "shutdown")));
#else
    enif_send(NULL, &pid, env,
              enif_make_tuple2(env, enif_make_atom(env, "error"),
                               enif_make_atom(env, "shutdown")));
#endif
    req->fn_post(req->args);
    enif_free(req->args);
    enif_free(req);
    enif_clear_env(env);
    async_nif_req_count--;
  }
  enif_free_env(env);
  enif_mutex_unlock(async_nif_req_mutex);

  /* Join for the now exiting worker threads. */
  for (i = 0; i < ASYNC_NIF_MAX_WORKERS; ++i) {
    enif_free_env(async_nif_worker_entries[i].env);
  }

  memset(async_nif_worker_entries, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS, 0);
  enif_cond_destroy(async_nif_cnd); async_nif_cnd = NULL;
  enif_mutex_destroy(async_nif_req_mutex); async_nif_req_mutex = NULL;
  enif_mutex_destroy(async_nif_worker_mutex); async_nif_worker_mutex = NULL;
}

static int async_nif_init(void)
{
  int i;

  /* Don't init more than once. */
  if (async_nif_req_mutex) return 0;

  async_nif_req_mutex = enif_mutex_create(NULL);
  async_nif_worker_mutex = enif_mutex_create(NULL);
  async_nif_cnd = enif_cond_create(NULL);

  /* Setup the requests management. */
  async_nif_req_count = 0;

  /* Setup the thread pool management. */
  enif_mutex_lock(async_nif_worker_mutex);
  memset(async_nif_worker_entries, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS, 0);

  for (i = 0; i < ASYNC_NIF_MAX_WORKERS; i++) {
    enif_thread_create(NULL, &async_nif_worker_entries[i].tid,
                       &async_nif_worker_fn, (void*)&async_nif_worker_entries[i], NULL);
    ErlNifEnv *env = enif_alloc_env();
    if (!env) {
      async_nif_shutdown = 1;
      enif_cond_broadcast(async_nif_cnd);
      enif_mutex_unlock(async_nif_worker_mutex);
      while(i >= 0) {
        void *exit_value = 0; /* Ignore this. */
        enif_thread_join(async_nif_worker_entries[i].tid, &exit_value);
        if (async_nif_worker_entries[i].env)
          enif_free_env(async_nif_worker_entries[i].env);
        i--;
      }
      memset(async_nif_worker_entries, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS, 0);
      enif_cond_destroy(async_nif_cnd); async_nif_cnd = NULL;
      enif_mutex_destroy(async_nif_req_mutex); async_nif_req_mutex = NULL;
      enif_mutex_destroy(async_nif_worker_mutex); async_nif_worker_mutex = NULL;
      return -1;
    }
    async_nif_worker_entries[i].env = env;
  }
  enif_mutex_unlock(async_nif_worker_mutex);
  return 0;
}

#if defined(__cplusplus)
}
#endif

#endif // __ASYNC_NIF_H__
