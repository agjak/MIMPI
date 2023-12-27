/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

}

void MIMPI_Finalize() {

    char* name1 = malloc(32*sizeof(char));
    char* name2 = malloc(32*sizeof(char));
    for(int i=0; i< MIMPI_World_size(); i++)
    {
        if(i!=MIMPI_World_rank())
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
    
    
    channels_finalize();
}

int MIMPI_World_size() {
    return atoi(getenv("MIMPI_world_size"));
}

int MIMPI_World_rank() {
    return atoi(getenv("MIMPI_world_rank"));
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    if (destination == MIMPI_World_rank())
    {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if (destination < 0 || destination >= MIMPI_World_size())
    {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    char* name = malloc(32*sizeof(char));
    sprintf(name, "MIMPI_channel_to_%d",destination);
    int send_fd=atoi(getenv(name));

    chsend(send_fd, data, count);
    free(name);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    if (source == MIMPI_World_rank())
    {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if (source < 0 || source >= MIMPI_World_size())
    {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    char* name = malloc(32*sizeof(char));
    sprintf(name, "MIMPI_channel_from_%d",source);
    int recv_fd=atoi(getenv(name));

    chrecv(recv_fd, data, count);
    free(name);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}