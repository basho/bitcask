%% -------------------------------------------------------------------
%%
%% fault injection primitives
%%
%% Copyright (c) 2014 Basho Technologies, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(trigger_commonpaths).
-export([config/0]).

-include("faulterl.hrl").

config() ->
    DoOnlyOnceHack = "
#ifndef TRIGGER_COMMONPATHS
#define TRIGGER_COMMONPATHS
static char *interesting_strings[] = {
    /* \"generic.qc\", /* Used by generic_qc_fsm.erl */
    /* Interesting bitcask things */
    \"bitcask.data\",
    \"bitcask.hint\",
    NULL
};

static int flock_op_array[] = { LOCK_SH, LOCK_EX, 0 };
static int fcntl_cmd_array[] = { F_SETLK, F_SETLKW, 0 };
static int open_write_op_array[] = { O_WRONLY, O_RDWR, 0 };
#endif
",

    %% MEMO: PathC_ means "path checking via strstr"
    PathC_CHeaders = ["<string.h>", "<stdio.h>"],
    PathC_TStruct = "
typedef struct {
    char *i_name; /* Must be present */
}",

    PathC_TNewInstanceArgs = "char *strstr_1st_arg",
    PathC_TNewInstanceBody = "
",
    %% NOTE: 'path' below matches last arg to config()
    PathC_TBody = "
    int i;

    res = 0;
    for (i = 0; interesting_strings[i] != NULL; i++) {
        if (strstr(path, interesting_strings[i]) != NULL) {
            res++;
            break;
        }
    }
",

    %% Memo: BitC_ means "bit checking", e.g. open(2) flags, flock(2) operation
    BitC_CHeaders = ["<sys/file.h>"],
    BitC_TStruct = "
typedef struct {
    char *i_name;  /* Must be present */
    int array[11]; /* Array of bit patterns to check, 0 signals end of array */
}",
    BitC_TNewInstanceArgs = "int *array",
    BitC_TNewInstanceBody = "
    int i;

    for (i = 0; array[i] != 0 && i < sizeof(a->array); i++) {
        a->array[i] = array[i];
    }
    a->array[i] = 0;
",
    %% NOTE: 'path' below matches last arg to config()
    BitC_TBody = "
    int i;

    res = 0;
    for (i = 0; a->array[i] != 0; i++) {
        if (a->array[i] & checked_operation) {
            res++;
            break;
        }
    }
",

    %% Memo: IntArgC_ means "integer arg checking"
    IntArgC_CHeaders = ["<sys/fcntl.h>"],
    IntArgC_TStruct = "
typedef struct {
    char *i_name;  /* Must be present */
    int array[11]; /* Array of ints to check, 0 signals end of array */
}",
    IntArgC_TNewInstanceArgs = "int *array",
    IntArgC_TNewInstanceBody = "
    int i;

    for (i = 0; array[i] != 0 && i < sizeof(a->array); i++) {
        a->array[i] = array[i];
    }
    a->array[i] = 0;
",
    %% NOTE: 'path' below matches last arg to config()
    IntArgC_TBody = "
    int i;

    res = 0;
    for (i = 0; a->array[i] != 0; i++) {
        if (a->array[i] == cmd) {
            res++;
            break;
        }
    }
",

    [trigger_args_of_intercept:config(
         PathC_CHeaders, [DoOnlyOnceHack], [],
         true, true, false,
         PathC_TStruct, "", PathC_TNewInstanceArgs, PathC_TNewInstanceBody,
         InterceptName, PathC_TBody,
         InterceptName, "path") || InterceptName <- ["access", "stat",
                                                     "open",
                                                     "unlink", "unlinkat",
                                                     "rename"]]
    ++
    [trigger_args_of_intercept:config(
         BitC_CHeaders, [], [],
         true, true, false,
         BitC_TStruct, "", BitC_TNewInstanceArgs, BitC_TNewInstanceBody,
         InterceptName, BitC_TBody,
         InterceptName, "bits") || InterceptName <- ["flock"]]
    ++
    [trigger_args_of_intercept:config(
         IntArgC_CHeaders, [], [],
         true, true, false,
         IntArgC_TStruct, "", IntArgC_TNewInstanceArgs, IntArgC_TNewInstanceBody,
         InterceptName, IntArgC_TBody,
         InterceptName, "cmd") || InterceptName <- ["fcntl"]]
    ++
    [trigger_random:config()]
    ++
    [
     %% A quick glance at the code suggests that the LevelDB code never
     %% checks the return status of a variable.  {shrug}

     %% HRM, seems like none of the EUnit tests trigger calls to access()
     %% by the leveldb code.  There are plenty of calls by the VM code
     %% loader and by the eqc framework, though.
     #fi{	% both?/OS X version
         name = "access",
         type = intercept,
         intercept_args = "const char *path, int amode",
         intercept_args_call = "path, amode",
         c_headers = ["<unistd.h>"],
         intercept_errno = "EFAULT",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_access_path", "\"-unused-arg-\""},
                               {"random", "percent_7", "7"}]
     },
     #fi{	% both?/OS X version
         name = "stat",
         type = intercept,
         intercept_args = "const char *path, struct stat *buf",
         intercept_args_call = "path, buf",
         c_headers = ["<sys/stat.h>"],
         intercept_errno = "EIO",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_stat_path", "\"-unused-arg-\""},
                               {"random", "percent_7", "4"}]
     },
     #fi{	% both?/OS X version
         name = "flock",
         type = intercept,
         intercept_args = "int fd, int checked_operation",
         intercept_args_call = "fd, checked_operation",
         c_headers = ["<sys/file.h>"],
         intercept_errno = "EDEADLK",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_flock_bits", "flock_op_array"},
                               {"random", "", "7"}]
     },
     #fi{	% both?/OS X version
         name = "fcntl",
         type = intercept,
         intercept_args = "int filedes, int cmd, ...",
         intercept_args_call = "filedes, cmd, arg",
         intercept_body_setup = "    int arg; va_list ap;\n\n" ++
                                "    va_start(ap, cmd); " ++
                                "arg = va_arg(ap, int); va_end(ap);\n",
         c_headers = ["<sys/file.h>"],
         intercept_errno = "EDEADLK",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_fcntl_cmd", "fcntl_cmd_array"},
                               {"random", "", "7"}]
     },
     #fi{	% both?/OS X version
         name = "open",
         type = intercept,
         intercept_args = "const char *path, int checked_operation, ...",
         intercept_args_call = "path, checked_operation, mode",
         intercept_body_setup = "    int mode; va_list ap;\n\n" ++
                                "    va_start(ap, checked_operation); " ++
                                "mode = va_arg(ap, int); va_end(ap);\n",
         c_headers = ["<fcntl.h>", "<stdarg.h>"],
         intercept_errno = "ENFILE",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_open_path", "\"-unused-arg-\""},
                               {"random", "", "7"}]
     },
     #fi{	% OS X version
         name = "opendir$INODE64",
         type = intercept,
         intercept_args = "const char *path",
         intercept_args_call = "path",
         c_headers = ["<dirent.h>"],
         %%%% intercept_errno = "EINTR",
         intercept_errno = "EMFILE",
         intercept_return_type = "DIR *",
         intercept_return_value = "NULL",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"random", "", "7"}]
     },
     %% Yup, ftruncate triggers the same bug during open that others do.
     #fi{	% both?/OS X version
         name = "ftruncate",
         type = intercept,
         intercept_args = "int fd, off_t length",
         intercept_args_call = "fd, length",
         c_headers = ["<unistd.h>"],
         intercept_errno = "ENOSPC",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"random", "", "0"}]
     },
     %% HRM, pread() is heavily used by EQC via dets, bummer.
     %% We need more smarts here, somehow.
     %% If nbyte > 9000, then we seem to avoid most/all of dets 8KB
     %% ?CHUNK_SIZE ops?
     #fi{	% both?/OS X version
         name = "pread",
         type = intercept,
         intercept_args = "int fd, void *buf, size_t nbyte, off_t offset",
         intercept_args_call = "fd, buf, nbyte, offset",
         c_headers = ["<unistd.h>"],
         intercept_errno = "EIO",
         intercept_return_type = "ssize_t",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"random", "", "0"}]
     },
     #fi{	% OS X version
         name = "unlink",
         type = intercept,
         intercept_args = "const char *path",
         intercept_args_call = "path",
         c_headers = ["<unistd.h>"],
         intercept_errno = "ENOSPC",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_unlink_path", "\"-unused-arg-\""},
                               {"random", "percent_7", "7"}]
     },
     #fi{
         name = "unlinkat",	% Linux 3.2 version
         type = intercept,
         intercept_args = "int __fd, __const char *path, int __flag",
         intercept_args_call = "__fd, path, __flag",
         c_headers = ["<unistd.h>"],
         intercept_errno = "ENOSPC",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_unlinkat_path", "\"-unused-arg-\""},
                               {"random", "percent_7", "7"}]
     },
     #fi{	% OS X version
         name = "rename",
         type = intercept,
         %% We'll re-use the same strstr trigger using the arg named 'path'
         %% Which means we only check for interesting names in the old path
         %% and that we ignore the new path.
         intercept_args = "const char *path, const char *new",
         intercept_args_call = "path, new",
         c_headers = ["<unistd.h>"],
         intercept_errno = "ENOSPC",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_rename_path", "\"-unused-arg-\""},
                               {"random", "percent_7", "7"}]
     }
    ].
