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

    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    MIMPI_send_finished_sync_signal_to_left_child(rank,size);
    MIMPI_send_finished_sync_signal_to_right_child(rank,size);
    MIMPI_send_finished_sync_signal_to_your_parent(rank);

    MIMPI_close_all_program_channels(rank,size);
    
    
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
    if(atoi(getenv("MIMPI_remotes_finished"))>0)
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();
    printf("Before %d\n", rank);
    //MIMPI_send_barrier_sync_signal_to_both_children(rank, size);
    MIMPI_send_barrier_sync_signal_to_right_child(rank, size);
    MIMPI_send_barrier_sync_signal_to_left_child(rank, size);
    printf("After %d\n", rank);

    char* messch1 = malloc(1*sizeof(char));
    messch1[0] = 'E';                       //EMPTY
    char* messch2 = malloc(1*sizeof(char));
    messch2[0] = 'E';                       //EMPTY
    char* messpar = malloc(1*sizeof(char));
    messpar[0] = 'E';                       //EMPTY

    if(rank*2+1<size)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_from_%d",rank*2+1);
        int recv_fd=atoi(getenv(name));
        void* mess = malloc(1*sizeof(char));
        chrecv(recv_fd,mess,1);
        messch1[0]= ((char*) mess)[0];
        free(name);
        free(mess);
    }
    if(rank*2+2<size)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_from_%d",rank*2+2);
        int recv_fd=atoi(getenv(name));
        void* mess = malloc(1*sizeof(char));
        chrecv(recv_fd,mess,1);
        messch2[0] = ((char*) mess)[0];
        free(name);
        free(mess);
    }
    if(rank!=0)
    {
        char* name = malloc(40*sizeof(char));
        sprintf(name, "MIMPI_sync_channel_from_%d",(rank-1)/2);
        int recv_fd=atoi(getenv(name));
        void* mess = malloc(1*sizeof(char));
        chrecv(recv_fd,mess,1);
        messpar[0] = ((char*) mess)[0];
        free(name);
        free(mess);
    }

    if(rank==0)
    {
        if(messch1[0]=='F' || messch2[0]=='F')   //parent or children have already finished the MIMPI block
        {
            free(messch1);
            free(messch2);
            free(messpar);
            MIMPI_send_finished_sync_signal_to_both_children(rank, size);
            setenv("MIMPI_remotes_finished","1",1);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else
        {
            free(messch1);
            free(messch2);
            free(messpar);
            //MIMPI_send_barrier_sync_signal_to_both_children(rank, size);
            MIMPI_send_barrier_sync_signal_to_right_child(rank, size);
            MIMPI_send_barrier_sync_signal_to_left_child(rank, size);
            return MIMPI_SUCCESS;
        }
    }
    else
    {
        if(messch1[0]=='F' || messch2[0]=='F' || messpar[0]=='F')   //parent or children have already finished the MIMPI block
        {
            free(messch1);
            free(messch2);
            free(messpar);
            MIMPI_send_finished_sync_signal_to_both_children_and_parent(rank, size);
            setenv("MIMPI_remotes_finished","1",1);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else    //parent and both children have started MIMPI_Barrier
        {
            char* name = malloc(40*sizeof(char));
            sprintf(name, "MIMPI_sync_channel_to_%d",(rank-1)/2);
            int send_fd=atoi(getenv(name));
            char* mess = malloc(1*sizeof(char));
            mess[0] = 'B'; //BARRIER
            chsend(send_fd, (void*) mess, 1);
            free(mess);
            
            sprintf(name, "MIMPI_sync_channel_from_%d",(rank-1)/2);
            int recv_fd=atoi(getenv(name));
            void* mess2 = malloc(1*sizeof(char));
            chrecv(recv_fd,mess2,1);
            messpar[0] = ((char*) mess2)[0];
            free(name);
            free(mess2);
            
            if(messpar[0]=='F')
            {
                free(messch1);
                free(messch2);
                free(messpar);
                MIMPI_send_finished_sync_signal_to_both_children(rank, size);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                free(messch1);
                free(messch2);
                free(messpar);
                //MIMPI_send_barrier_sync_signal_to_both_children(rank, size);
                MIMPI_send_barrier_sync_signal_to_right_child(rank, size);
                MIMPI_send_barrier_sync_signal_to_left_child(rank, size);
                return MIMPI_SUCCESS;
            }

        }
    }
    
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