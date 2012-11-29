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

#include "queue.h"

struct async_nif_req_entry {
  ERL_NIF_TERM ref, *argv;
  ErlNifEnv *env;
  ErlNifPid pid;
  void *args;
  void *priv_data;
  void (*fn_work)(ErlNifEnv*, ERL_NIF_TERM, void *, ErlNifPid*, void *);
  void (*fn_post)(void *);
  STAILQ_ENTRY(async_nif_req_entry) entries;
};
STAILQ_HEAD(reqs, async_nif_req_entry) async_nif_reqs = STAILQ_HEAD_INITIALIZER(async_nif_reqs);

struct async_nif_worker_entry {
  ErlNifTid tid;
  LIST_ENTRY(async_nif_worker_entry) entries;
};
LIST_HEAD(idle_workers, async_nif_worker_entry) async_nif_idle_workers = LIST_HEAD_INITIALIZER(async_nif_worker);

static volatile unsigned int async_nif_req_count = 0;
static volatile unsigned int async_nif_shutdown = 0;
static ErlNifMutex *async_nif_req_mutex = NULL;
static ErlNifMutex *async_nif_worker_mutex = NULL;
static ErlNifCond *async_nif_cnd = NULL;
static struct async_nif_worker_entry *async_nif_worker_entries = NULL;
static unsigned int async_nif_worker_count = 0;


#define ASYNC_NIF_DECL(decl, frame, pre_block, work_block, post_block)  \
  struct decl ## _args frame;                                           \
  static void fn_work_ ## decl (ErlNifEnv *env, ERL_NIF_TERM ref, void *priv_data, ErlNifPid *pid, struct decl ## _args *args) work_block \
  static void fn_post_ ## decl (struct decl ## _args *args) {           \
    do post_block while(0);                                             \
  }                                                                     \
  static ERL_NIF_TERM decl(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv_in[]) { \
    struct decl ## _args on_stack_args;                                 \
    struct decl ## _args *args = &on_stack_args;                        \
    struct decl ## _args *copy_of_args;                                 \
    struct async_nif_req_entry *req = NULL;                             \
    ErlNifEnv *new_env = NULL;                                          \
    /* argv[0] is a ref used for selective recv */                      \
    const ERL_NIF_TERM *argv = argv_in + 1;                             \
    argc--;                                                             \
    if (async_nif_shutdown)                                             \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "shutdown"));         \
    if (!(new_env = enif_alloc_env())) {                                \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "enomem"));           \
    }                                                                   \
    do pre_block while(0);                                              \
    req = (struct async_nif_req_entry*)enif_alloc(sizeof(struct async_nif_req_entry)); \
    if (!req) {                                                         \
      fn_post_ ## decl (args);                                          \
      enif_free_env(new_env);                                           \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "enomem"));           \
    }                                                                   \
    copy_of_args = (struct decl ## _args *)enif_alloc(sizeof(struct decl ## _args)); \
    if (!copy_of_args) {                                                \
      fn_post_ ## decl (args);                                          \
      enif_free_env(new_env);                                           \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "enomem"));           \
    }                                                                   \
    memcpy(copy_of_args, args, sizeof(struct decl ## _args));           \
    req->env = new_env;                                                 \
    req->ref = enif_make_copy(new_env, argv_in[0]);                     \
    enif_self(env, &req->pid);                                          \
    req->args = (void*)copy_of_args;                                    \
    req->priv_data = enif_priv_data(env);                               \
    req->fn_work = (void (*)(ErlNifEnv *, ERL_NIF_TERM, void*, ErlNifPid*, void *))fn_work_ ## decl ; \
    req->fn_post = (void (*)(void *))fn_post_ ## decl;                  \
    async_nif_enqueue_req(req);                                         \
    return enif_make_tuple2(env, enif_make_atom(env, "ok"),             \
                            enif_make_tuple2(env, enif_make_atom(env, "enqueued"), \
                                             enif_make_int(env, async_nif_req_count))); \
  }

#define ASYNC_NIF_LOAD(count) if (async_nif_init(count) != 0) return -1;
#define ASYNC_NIF_UNLOAD() async_nif_unload();
#define ASYNC_NIF_UPGRADE() async_nif_unload();

#define ASYNC_NIF_RETURN_BADARG() return enif_make_badarg(env);
#define ASYNC_NIF_WORK_ENV new_env

#ifndef PULSE_FORCE_USING_PULSE_SEND_HERE
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
      req->fn_work(req->env, req->ref, req->priv_data, &req->pid, req->args);
      req->fn_post(req->args);
      enif_free(req->args);
      enif_free_env(req->env);
      enif_free(req);
    }
  }
  enif_thread_exit(0);
  return 0;
}

static int async_nif_size_thread_pool(int amt)
{
  int i;

  /* Setup the thread pool management. */
  enif_mutex_lock(async_nif_worker_mutex);
  if (amt == 0) {
    enif_mutex_unlock(async_nif_worker_mutex);
    return 0;
  } else if (amt < 0) {
    if (abs(amt) != async_nif_worker_count) {
      /* TODO: Someday add code to shrink the number of worker threads, ...
         but not today. */
      enif_mutex_unlock(async_nif_worker_mutex);
      return -1;
    } else {
      /* Signal the worker threads, stop what you're doing and exit. */
      async_nif_shutdown = 1;
      enif_cond_broadcast(async_nif_cnd);
      enif_mutex_unlock(async_nif_req_mutex);

      /* Join for the now exiting worker threads. */
      i = async_nif_worker_count;
      while(i) {
        void *exit_value = 0; /* Ignore this. */
        enif_thread_join(async_nif_worker_entries[--i].tid, &exit_value);
      }
      async_nif_worker_count = 0;
    }
  } else {
    if (async_nif_worker_count == 0) {
      /* Create a new pool. */
      async_nif_worker_entries = enif_alloc(sizeof(struct async_nif_worker_entry) * amt);
    } else {
      /* Growing the thread pool from `async_nif_worker_count` by `amt` new workers. */
      struct async_nif_worker_entry *new = enif_realloc(async_nif_worker_entries, sizeof(struct async_nif_worker_entry) * amt);
      async_nif_worker_entries = new; /* It could have moved, but that should be ok. */
    }
    memset(async_nif_worker_entries + (async_nif_worker_count * sizeof(struct async_nif_worker_entry)), sizeof(struct async_nif_worker_entry) * abs(amt), 0);
    for (i = 0; i < amt; i++) {
      if (enif_thread_create(NULL, &async_nif_worker_entries[async_nif_worker_count + 1].tid,
                             &async_nif_worker_fn, (void*)&async_nif_worker_entries[async_nif_worker_count + 1], NULL) != 0) {
        /* If things went wrong growing, we shutdown all worker threads not just the new ones. */
        enif_mutex_unlock(async_nif_worker_mutex);
        async_nif_size_thread_pool(-async_nif_worker_count);
        return -1;
      }
      async_nif_worker_count += 1;
    }
  }
  enif_mutex_unlock(async_nif_worker_mutex);
  return 0;
}

static void async_nif_unload(void)
{
  /* Shutdown the worker threads, free up that memory. */
  async_nif_size_thread_pool(-async_nif_worker_count);
  enif_free(async_nif_worker_entries);
  async_nif_worker_entries = 0;

  /* Worker threads are stopped, now toss out anything left in the queue. */
  enif_mutex_lock(async_nif_req_mutex);
  struct async_nif_req_entry *req = NULL;
  STAILQ_FOREACH(req, &async_nif_reqs, entries) {
    STAILQ_REMOVE(&async_nif_reqs, STAILQ_LAST(&async_nif_reqs, async_nif_req_entry, entries),
                  async_nif_req_entry, entries);
#ifdef PULSE
    PULSE_SEND(NULL, &req->pid, req->env,
              enif_make_tuple2(req->env, enif_make_atom(req->env, "error"),
                               enif_make_atom(req->env, "shutdown")));
#else
    enif_send(NULL, &req->pid, req->env,
              enif_make_tuple2(req->env, enif_make_atom(req->env, "error"),
                               enif_make_atom(req->env, "shutdown")));
#endif
    req->fn_post(req->args);
    enif_free(req->args);
    enif_free(req);
    async_nif_req_count--;
  }
  enif_mutex_unlock(async_nif_req_mutex);

  enif_cond_destroy(async_nif_cnd); async_nif_cnd = NULL;
  enif_mutex_destroy(async_nif_req_mutex); async_nif_req_mutex = NULL;
  enif_mutex_destroy(async_nif_worker_mutex); async_nif_worker_mutex = NULL;
}

static int async_nif_init(unsigned int count)
{

  /* Don't init more than once. */
  if (async_nif_req_mutex) return 0;

  async_nif_req_mutex = enif_mutex_create(NULL);
  async_nif_worker_mutex = enif_mutex_create(NULL);
  async_nif_cnd = enif_cond_create(NULL);
  async_nif_req_count = 0;
  return async_nif_size_thread_pool(count);
}

#if defined(__cplusplus)
}
#endif

#endif // __ASYNC_NIF_H__
