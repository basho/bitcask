// ---------------------------------------------------------
// - Small utility lib for sending messages from C through
// - the PULSE scheduler.
// -
// - The caveat is that you can't send messages to a named/
// - registered process from a NIF. Therefore a little bit
// - of machinery is used to keep track of the Pid of PULSE
// - and to enable message sends to PULSE via the pulse_send
// - function.
// -
// ----------------------------------------------------------

#ifndef PULSE_C_SEND_H
#define PULSE_C_SEND_H

#include "erl_nif.h"

#define PULSE_SEND(env, dest_pid, msg_env, msg) \
  pulse_send(env, dest_pid, msg_env, msg, __FILE__, __LINE__)

ERL_NIF_TERM set_pulse_pid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

int  pulse_send(ErlNifEnv* env, ErlNifPid* dest_pid,
                ErlNifEnv* msg_env, ERL_NIF_TERM msg,
                char* file, int line);

int pulse_c_send_on_load(ErlNifEnv* env);

#endif
