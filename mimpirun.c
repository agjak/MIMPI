/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"

int main(int argc, char* argv[]) {
    if (argc<3)
    {
        fatal("Usage: %s <num_of_copies> <name_of_prog_to_run> <args>\n", argv[0]);
    }
    int world_size=atoi(argv[1]);
    char* prog_name=argv[2];
    char** prog_args = &argv[3];

    ASSERT_SYS_OK(setenv("MIMPI_world_size", argv[1], 1));

    channels_init();

    int** read_pipes = malloc(world_size*sizeof(int*));
    int** write_pipes = malloc(world_size*sizeof(int*));
    for(int i=0; i<world_size; i++)
    {
        read_pipes[i] = malloc(world_size*sizeof(int));
        write_pipes[i] = malloc(world_size*sizeof(int));
    }

    for(int i=0; i<world_size; i++)
    {
        for(int j=0; j<world_size; j++)
        {
            if(i==j)
            {
                read_pipes[i][i]=0;
                write_pipes[i][i]=0;
            }
            else
            {
                int fds[2];
                ASSERT_SYS_OK(channel(fds));
                read_pipes[i][j]=fds[0];    //read and write ends of a pipe from the ith process to the jth
                write_pipes[i][j]=fds[1];   //process are saved here
            }
            
        }
    }
    char* name1 = malloc(32*sizeof(char));
    char* name2 = malloc(32*sizeof(char));
    char* value1 = malloc(12*sizeof(char));
    char* value2 = malloc(12*sizeof(char));
    char* world_rank = malloc(12*sizeof(char));
    for (int i=0; i<world_size; i++)
    {
        sprintf(world_rank, "%d", i);
        ASSERT_SYS_OK(setenv("MIMPI_world_rank", world_rank, 1));

        for(int j=0; j<world_size; j++)
        {
            if(j!=i)
            {
                sprintf(name1, "MIMPI_channel_from_%d",j);
                sprintf(value1, "%d", write_pipes[j][i]);
                sprintf(name2, "MIMPI_channel_to_%d",j);
                sprintf(value2, "%d", read_pipes[i][j]);
            }
        }

        pid_t pid;
        ASSERT_SYS_OK(pid = fork());
        if(!pid)
        {
            for(int j=0; j<world_size; j++)
            {
                for(int k=0; k<world_size; k++)
                {
                    if(j!=k && j!=i)
                    {
                        close(read_pipes[i][j]);
                        close(write_pipes[j][i]);
                    }
                }
            }
            execvp(prog_name, prog_args);
        }
    }
    free(name1);
    free(name2);
    free(value1);
    free(value2);
    free(world_rank);

    for(int i=0; i<world_size; i++)
    {
        for(int j=0; j<world_size; j++)
        {
            if(i!=j)
            {
                close(read_pipes[i][j]);
                close(write_pipes[i][j]);
            }
        }
    }

    for(int i=0; i<world_size; i++)
    {
        free(read_pipes[i]);
        free(write_pipes[i]);
    }
    free(read_pipes);
    free(write_pipes);

    channels_finalize();

    for(int i=0; i<world_size; i++)
    {
        ASSERT_SYS_OK(wait(NULL));
    }
    return 0;
}