/**
 * This file is for implementation of common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#include "mimpi_common.h"


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
        }
    }
    free(name1);
    free(name2);
}




