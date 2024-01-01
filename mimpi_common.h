/**
 * This file is for declarations of  common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#ifndef MIMPI_COMMON_H
#define MIMPI_COMMON_H

#include "channel.h"

#include <assert.h>
#include <stdbool.h>
#include <stdnoreturn.h>

#include <stdlib.h>
#include <sys/types.h>
#include <stddef.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>

/*
    Assert that expression doesn't evaluate to -1 (as almost every system function does in case of error).

    Use as a function, with a semicolon, like: ASSERT_SYS_OK(close(fd));
    (This is implemented with a 'do { ... } while(0)' block so that it can be used between if () and else.)
*/
#define ASSERT_SYS_OK(expr)                                                                \
    do {                                                                                   \
        if ((expr) == -1)                                                                  \
            syserr(                                                                        \
                "system command failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ", \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Assert that expression evaluates to zero (otherwise use result as error number, as in pthreads). */
#define ASSERT_ZERO(expr)                                                                  \
    do {                                                                                   \
        int const _errno = (expr);                                                         \
        if (_errno != 0)                                                                   \
            syserr(                                                                        \
                "Failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ",                \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Prints with information about system error (errno) and quits. */
_Noreturn extern void syserr(const char* fmt, ...);

/* Prints (like printf) and quits. */
_Noreturn extern void fatal(const char* fmt, ...);

#define TODO fatal("UNIMPLEMENTED function %s", __PRETTY_FUNCTION__);


/////////////////////////////////////////////
// Put your declarations here

typedef enum {
    MIMPI_SUCCESS = 0, /// operation ended successfully
    MIMPI_ERROR_ATTEMPTED_SELF_OP = 1, /// process attempted to send/recv to itself
    MIMPI_ERROR_NO_SUCH_RANK = 2, /// no process with requested rank exists in the world
    MIMPI_ERROR_REMOTE_FINISHED = 3, /// the remote process involved in communication has finished
    MIMPI_ERROR_DEADLOCK_DETECTED = 4, /// a deadlock has been detected
} MIMPI_Sync_Retcode;


MIMPI_Sync_Retcode MIMPI_send_sync_signal_to_parent(int rank, char signal);
MIMPI_Sync_Retcode MIMPI_send_sync_signal_to_left_child(int rank, int size, char signal);
MIMPI_Sync_Retcode MIMPI_send_sync_signal_to_right_child(int rank, int size, char signal);
MIMPI_Sync_Retcode MIMPI_send_sync_signal_to_both_children(int rank, int size, char signal);

MIMPI_Sync_Retcode MIMPI_sync_send(char signal, int destination);
MIMPI_Sync_Retcode MIMPI_sync_recv(char* signal, int source);

void MIMPI_close_all_program_channels(int rank, int size);




#endif // MIMPI_COMMON_H