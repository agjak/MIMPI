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
    char** prog_args = &argv[2];

    ASSERT_SYS_OK(setenv("MIMPI_world_size", argv[1], 1));
    


    int** read_channels_between_programs = malloc(world_size*sizeof(int*));
    int** write_channels_between_programs = malloc(world_size*sizeof(int*));
    int** read_sync_channels_between_programs = malloc(world_size*sizeof(int*));
    int** write_sync_channels_between_programs = malloc(world_size*sizeof(int*));

    for(int i=0; i<world_size; i++)
    {
        read_channels_between_programs[i] = malloc(world_size*sizeof(int));
        write_channels_between_programs[i] = malloc(world_size*sizeof(int));
        read_sync_channels_between_programs[i] = malloc(world_size*sizeof(int));
        write_sync_channels_between_programs[i] = malloc(world_size*sizeof(int));
    }

    int free_fd=20;
    for(int i=0; i<world_size; i++)
    {
        for(int j=0; j<world_size; j++)
        {
            if(i==j)
            {
                read_channels_between_programs[i][i]=0;
                write_channels_between_programs[i][i]=0;
                read_sync_channels_between_programs[i][i]=0;
                write_sync_channels_between_programs[i][i]=0;
            }
            else
            {
                int fds[2];
                ASSERT_SYS_OK(channel(fds));
                dup2(fds[0],free_fd);
                close(fds[0]);
                read_channels_between_programs[i][j]=free_fd;    //read and write ends of pipes from the ith process to the jth
                free_fd++;                                      //process are saved here
                dup2(fds[1],free_fd);
                close(fds[1]);
                write_channels_between_programs[i][j]=free_fd; 
                free_fd++;  
                ASSERT_SYS_OK(channel(fds));
                dup2(fds[0],free_fd);
                close(fds[0]);
                read_sync_channels_between_programs[i][j]=free_fd;       //the first pipe is for data transfer, the second is for synchronization
                free_fd++;
                dup2(fds[1],free_fd);
                close(fds[1]);
                write_sync_channels_between_programs[i][j]=free_fd;
                free_fd++;   
            }
            
        }
    }

    char* name1 = malloc(38*sizeof(char));
    char* name2 = malloc(38*sizeof(char));
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
                sprintf(value1, "%d", read_channels_between_programs[j][i]);
                sprintf(name2, "MIMPI_channel_to_%d",j);
                sprintf(value2, "%d", write_channels_between_programs[i][j]);
                ASSERT_SYS_OK(setenv(name1, value1, 1));
                ASSERT_SYS_OK(setenv(name2, value2, 1));

                sprintf(name1, "MIMPI_sync_channel_from_%d",j);
                sprintf(value1, "%d", read_sync_channels_between_programs[j][i]);
                sprintf(name2, "MIMPI_sync_channel_to_%d",j);
                sprintf(value2, "%d", write_sync_channels_between_programs[i][j]);
                ASSERT_SYS_OK(setenv(name1, value1, 1));
                ASSERT_SYS_OK(setenv(name2, value2, 1));
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
                        close(read_channels_between_programs[k][j]);
                        close(write_channels_between_programs[j][k]);
                        close(read_sync_channels_between_programs[k][j]);
                        close(write_sync_channels_between_programs[j][k]);
                    }
                }
            }
            for(int i=0; i<world_size; i++)
            {
                free(read_channels_between_programs[i]);
                free(write_channels_between_programs[i]);
                free(read_sync_channels_between_programs[i]);
                free(write_sync_channels_between_programs[i]);
            }
            free(read_channels_between_programs);
            free(write_channels_between_programs);
            free(read_sync_channels_between_programs);
            free(write_sync_channels_between_programs);

            free(name1);
            free(name2);
            free(value1);
            free(value2);
            free(world_rank);

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
                close(read_channels_between_programs[i][j]);
                close(write_channels_between_programs[i][j]);
                close(read_sync_channels_between_programs[i][j]);
                close(write_sync_channels_between_programs[i][j]);
            }
        }
    }

    for(int i=0; i<world_size; i++)
    {
        free(read_channels_between_programs[i]);
        free(write_channels_between_programs[i]);
        free(read_sync_channels_between_programs[i]);
        free(write_sync_channels_between_programs[i]);
    }
    free(read_channels_between_programs);
    free(write_channels_between_programs);
    free(read_sync_channels_between_programs);
    free(write_sync_channels_between_programs);


    for(int i=0; i<world_size; i++)
    {
        printf("A child has returned\n");
        ASSERT_SYS_OK(wait(NULL));
    }
    return 0;
}