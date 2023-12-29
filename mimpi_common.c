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

void MIMPI_send_sync_signal_to_parent(int rank, char signal)
{
    if(rank>0)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_to_%d",(rank-1)/2);
        int send_fd=atoi(getenv(name));
        char* mess = malloc(1*sizeof(char));
        mess[0] = signal;
        chsend(send_fd, (void*) mess, 1);
        free(name);
        free(mess);
    }
}



void MIMPI_send_sync_signal_to_both_children(int rank, int size, char signal)
{
    pid_t pid1;
    pid_t pid2;
    fflush(stdout);
    ASSERT_SYS_OK(pid1 = fork());
    if(!pid1)
    {
        MIMPI_send_sync_signal_to_left_child(rank,size,signal);
        MIMPI_close_all_program_channels(rank,size);
        exit(0);
    }
    else
    {
        ASSERT_SYS_OK(pid2 = fork());
        if(!pid2)
        {
            MIMPI_send_sync_signal_to_right_child(rank,size,signal);
            MIMPI_close_all_program_channels(rank,size);
            exit(0);
        }
        else
        {
            ASSERT_SYS_OK(wait(NULL));
            ASSERT_SYS_OK(wait(NULL));
        }
    }
}

void MIMPI_send_sync_signal_to_both_children_and_parent(int rank, int size, char signal)
{
    pid_t pid1;
    pid_t pid2;
    pid_t pid3;
    fflush(stdout);
    ASSERT_SYS_OK(pid1 = fork());
    if(!pid1)
    {
        MIMPI_send_sync_signal_to_left_child(rank,size,signal);
        MIMPI_close_all_program_channels(rank,size);
        exit(0);
    }
    else
    {
        ASSERT_SYS_OK(pid2 = fork());
        if(!pid2)
        {
            MIMPI_send_sync_signal_to_right_child(rank,size,signal);
            MIMPI_close_all_program_channels(rank,size);
            exit(0);
        }
        else
        {
            ASSERT_SYS_OK(pid3 = fork());
            if(!pid3)
            {
                MIMPI_send_sync_signal_to_parent(rank,signal);
                MIMPI_close_all_program_channels(rank,size);
                exit(0);
            }
            else
            {
                ASSERT_SYS_OK(wait(NULL));
                ASSERT_SYS_OK(wait(NULL));
                ASSERT_SYS_OK(wait(NULL));
            }
        }
    }
}

void MIMPI_send_sync_signal_to_left_child(int rank, int size, char signal)
{
    if(rank*2+1<size)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_to_%d",rank*2+1);
        int send_fd=atoi(getenv(name));
        char* mess = malloc(1*sizeof(char));
        mess[0] = signal;
        chsend(send_fd, (void*)(mess), 1);
        free(name);
        free(mess);
    }
}

void MIMPI_send_sync_signal_to_right_child(int rank, int size, char signal)
{
    if(rank*2+2<size)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_to_%d",rank*2+2);
        int send_fd=atoi(getenv(name));
        char* mess = malloc(1*sizeof(char));
        mess[0] = signal;
        chsend(send_fd, (void*) mess, 1);
        free(name);
        free(mess);
    }
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

void MIMPI_do_broadcast(void* data, int count, int root, int rank, int size)
{
    if(rank==root)
    {
        for(int i=0; i<size; i++)
        {
            if(i!=rank)
            {
                char* name = malloc(40*sizeof(char));
                sprintf(name, "MIMPI_sync_channel_to_%d",i);
                int send_fd=atoi(getenv(name));
                chsend(send_fd, data, count);
                free(name);
            }
        }
    }
    else
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_from_%d",root);
        int recv_fd=atoi(getenv(name));
        chrecv(recv_fd,data,count);
        free(name);
    }
}