/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"

int main(int argc, char* argv[]) {
    if (argc<3)
    {
        fatal("Usage: %s <num_of_copies> <name_of_prog_to_run> <args>\n", argv[0]);
    }
    int world_size=atoi(argv[1]);
    char* prog_name=argv[2];
    char** prog_args = &argv[3];

    ASSERT_SYS_OK(setenv("MIMPI_world_size", argv[1], 1));

    char* world_rank = malloc(2*sizeof(char));
    for (int i=0; i<world_size; i++)
    {
        
        ASSERT_SYS_OK(setenv("MIMPI_world_rank", itoa(i,world_rank,10), 1));
        pid_t pid;
        ASSERT_SYS_OK(pid = fork());
        if(!pid)
        {
            execvp(prog_name, prog_args);
        }
    }
    free(world_rank);

    for(int i=0; i<world_size; i++)
    {
        ASSERT_SYS_OK(wait(NULL));
    }
    return 0;
}