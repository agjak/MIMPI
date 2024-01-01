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

MIMPI_Sync_Retcode MIMPI_send_sync_signal_to_parent(int rank, char signal)
{
    if(rank>0)
    {
        return MIMPI_sync_send(signal, (rank-1)/2);
    }
    return MIMPI_SYNC_SUCCESS;
}



MIMPI_Sync_Retcode MIMPI_send_sync_signal_to_both_children(int rank, int size, char signal)
{
    pid_t pid1;
    pid_t pid2;
    fflush(stdout);
    ASSERT_SYS_OK(pid1 = fork());
    if(!pid1)
    {
        MIMPI_Sync_Retcode status = MIMPI_send_sync_signal_to_left_child(rank,size,signal);
        if (status==MIMPI_SYNC_SUCCESS)
        {
            exit(0);
        }
        else
        {
            exit(1);
        }
    }
    else
    {
        ASSERT_SYS_OK(pid2 = fork());
        if(!pid2)
        {
            MIMPI_Sync_Retcode status = MIMPI_send_sync_signal_to_right_child(rank,size,signal);
            if (status==MIMPI_SYNC_SUCCESS)
            {
                exit(0);
            }
            else
            {
                exit(1);
            }
        }
    }
    int status1;
    int status2;
    ASSERT_SYS_OK(wait(&status1));
    ASSERT_SYS_OK(wait(&status2));
    if(status1+status2==0)
    {
        return MIMPI_SYNC_SUCCESS;
    }
    else
    {
        return MIMPI_SYNC_ERROR_REMOTE_FINISHED;
    }
}


MIMPI_Sync_Retcode MIMPI_send_sync_signal_to_left_child(int rank, int size, char signal)
{
    if(rank*2+1<size)
    {
        return MIMPI_sync_send(signal, rank*2+1);
    }
    return MIMPI_SYNC_SUCCESS;
}

MIMPI_Sync_Retcode MIMPI_send_sync_signal_to_right_child(int rank, int size, char signal)
{
    if(rank*2+2<size)
    {
        return MIMPI_sync_send(signal, rank*2+2);
    }
    return MIMPI_SYNC_SUCCESS;
}

void MIMPI_close_all_program_channels(int rank, int size)
{
    char* name1 = malloc(40*sizeof(char));
    char* name2 = malloc(40*sizeof(char));

    for(int i=0; i< size; i++)
    {
        if(i!=rank)
        {
            sprintf(name1, "MIMPI_channel_to_%d", i);
            sprintf(name2, "MIMPI_channel_from_%d",i);
            int write_fd=atoi(getenv(name1));
            int read_fd=atoi(getenv(name2));
            close(write_fd);
            close(read_fd);

            sprintf(name1, "MIMPI_sync_channel_to_%d", i);
            sprintf(name2, "MIMPI_sync_channel_from_%d",i);
            write_fd=atoi(getenv(name1));
            read_fd=atoi(getenv(name2));
            close(write_fd);
            close(read_fd);
        }
    }
    free(name1);
    free(name2);
}


MIMPI_Sync_Retcode MIMPI_sync_send(
    char signal,
    int destination
) {
    char* name = malloc(40*sizeof(char));
    sprintf(name, "MIMPI_sync_channel_to_%d",destination);
    int send_fd=atoi(getenv(name));
    free(name);
    char* mess = malloc(1*sizeof(char));
    mess[0] = signal;
    
    if(chsend(send_fd, mess, 1)==-1)
    {
        free(mess);
        return MIMPI_SYNC_ERROR_REMOTE_FINISHED;
    }
    else
    {
        free(mess);
        return MIMPI_SYNC_SUCCESS;
    }
}

MIMPI_Sync_Retcode MIMPI_sync_recv(
    char* signal,
    int source
) {
    char* name = malloc(40*sizeof(char));
    sprintf(name, "MIMPI_sync_channel_from_%d",source);
    int recv_fd=atoi(getenv(name));
    free(name);

    if(chrecv(recv_fd, (void*)signal, 1)==0)
    {
        return MIMPI_SYNC_ERROR_REMOTE_FINISHED;
    }
    else
    {
        return MIMPI_SYNC_SUCCESS;
    }
}

