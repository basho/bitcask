// ---------------------------------------------------------
// - Small utility lib for sending messages from C through
// - the PULSE scheduler.
// -
// - The caveat is that you can't send messages to a named/
// - registered process from a NIF. Therefore a little bit
// - of machinery is used to keep track of the Pid of PULSE
// - and to enable message sends to PULSE via the pulse_send
// - function.
//
// ----------------------------------------------------------
#ifdef PULSE

#include "erl_nif.h"
#include "erl_nif_compat.h"
#include "pulse_c_send.h"

// The global place to store the pid of PULSE
ErlNifPid* THE_PULSE_PID;

// Send atom, initialized on_load
static ERL_NIF_TERM ATOM_SEND;
static ERL_NIF_TERM ATOM_OK;

ERL_NIF_TERM set_pulse_pid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]){
    if(!THE_PULSE_PID){
        THE_PULSE_PID = (ErlNifPid *)malloc(sizeof(ErlNifPid));
    }

    enif_get_local_pid(env, argv[0], THE_PULSE_PID);
    
    return ATOM_OK;
}

int  pulse_send(ErlNifEnv* env, ErlNifPid* dest_pid,
                ErlNifEnv* msg_env, ERL_NIF_TERM msg){
    ERL_NIF_TERM t_dest_pid = enif_make_pid(env, dest_pid);
    ERL_NIF_TERM pulse_msg = enif_make_tuple3(env,
                                              ATOM_SEND,
                                              t_dest_pid,
                                              msg);

    return enif_send(env, THE_PULSE_PID, msg_env, pulse_msg);
}

int pulse_c_send_on_load(ErlNifEnv* env){
    THE_PULSE_PID = (ErlNifPid *)0L;

    ATOM_SEND = enif_make_atom(env, "send");
    ATOM_OK = enif_make_atom(env, "ok");

    return 0;
}

#endif // ifdef PULSE
