/**
 * This file is for implementation of common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#include "mimpi_common.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

_Noreturn void syserr(const char* fmt, ...)
{
    va_list fmt_args;

    fprintf(stderr, "ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);
    fprintf(stderr, " (%d; %s)\n", errno, strerror(errno));
    exit(1);
}

_Noreturn void fatal(const char* fmt, ...)
{
    va_list fmt_args;

    fprintf(stderr, "ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);

    fprintf(stderr, "\n");
    exit(1);
}

/////////////////////////////////////////////////
// Put your implementation here

void MIMPI_send_finished_sync_signal_to_your_parent()
{
    int rank = atoi(getenv("MIMPI_world_rank"));
    if(rank>0)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_to_%d",(rank-1)/2);
        int send_fd=atoi(getenv(name));
        char* mess = malloc(1*sizeof(char));
        mess[0] = 'F'; //FINISHED
        chsend(send_fd, (void*) mess, 1);
        free(name);
        free(mess);
    }
}

void MIMPI_send_finished_sync_signal_to_your_children()
{
    int rank = atoi(getenv("MIMPI_world_rank"));
    int size = atoi(getenv("MIMPI_world_size"));
    if(rank*2+1<size)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_to_%d",rank*2+1);
        int send_fd=atoi(getenv(name));
        char* mess = malloc(1*sizeof(char));
        mess[0] = 'F'; //FINISHED
        chsend(send_fd, (void*)(mess), 1);
        free(name);
        free(mess);
    }
    if(rank*2+2<size)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_to_%d",rank*2+2);
        int send_fd=atoi(getenv(name));
        char* mess = malloc(1*sizeof(char));
        mess[0] = 'F'; //FINISHED
        chsend(send_fd, (void*) mess, 1);
        free(name);
        free(mess);
    }
}

void MIMPI_send_barrier_sync_signal_to_your_children()
{
    int rank = atoi(getenv("MIMPI_world_rank"));
    int size = atoi(getenv("MIMPI_world_size"));

    if(rank*2+1<size)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_to_%d",rank*2+1);
        int send_fd=atoi(getenv(name));
        char* mess = malloc(1*sizeof(char));
        mess[0] = 'B'; //BARRIER
        chsend(send_fd, (void*)(mess), 1);
        free(name);
        free(mess);
    }
    if(rank*2+2<size)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_to_%d",rank*2+2);
        int send_fd=atoi(getenv(name));
        char* mess = malloc(1*sizeof(char));
        mess[0] = 'B'; //BARRIER
        chsend(send_fd, (void*) mess, 1);
        free(name);
        free(mess);
    }
}
