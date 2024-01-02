/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <string.h>
#include <pthread.h>

uint8_t ***message_buffers;
pthread_mutex_t *buffer_mutexes;
pthread_t *buffer_threads;
pthread_cond_t *buffer_conditions;
int *messages_buffered;


MIMPI_Retcode MIMPI_sync_send(
    char signal,
    int destination
) 
{
    char* name = malloc(40*sizeof(char));
    sprintf(name, "MIMPI_sync_channel_to_%d",destination);
    int send_fd=atoi(getenv(name));
    free(name);
    char* mess = malloc(1*sizeof(char));
    mess[0] = signal;
    
    if(chsend(send_fd, mess, 1)==-1)
    {
        free(mess);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else
    {
        free(mess);
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_sync_recv(
    char* signal,
    int source
) 
{
    char* name = malloc(40*sizeof(char));
    sprintf(name, "MIMPI_sync_channel_from_%d",source);
    int recv_fd=atoi(getenv(name));
    free(name);

    if(chrecv(recv_fd, (void*)signal, 1)==0)
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else
    {
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_sync_reduce_send(
    char signal,
    int destination,
    uint8_t *data,
    int count
) 
{
    pid_t pid1;
    pid_t pid2;
    fflush(stdout);
    ASSERT_SYS_OK(pid1 = fork());
    if(!pid1)
    {
        MIMPI_Retcode status = MIMPI_sync_send(signal, destination);
        if (status==MIMPI_SUCCESS)
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
            MIMPI_Retcode status = MIMPI_Send(data, count, destination, -2);
            if (status==MIMPI_SUCCESS)
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
        return MIMPI_SUCCESS;
    }
    else
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
}

MIMPI_Retcode MIMPI_sync_reduce_recv(
    char* signal,
    int source,
    uint8_t *data,
    int count
) 
{
    char* name = malloc(40*sizeof(char));
    sprintf(name, "MIMPI_sync_channel_from_%d",source);
    int recv_fd=atoi(getenv(name));
    free(name);

    if(chrecv(recv_fd, (void*)signal, 1)==0)
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else
    {
        if(signal[0]=='F')
        {
            return MIMPI_SUCCESS;
        }
        else
        {
            MIMPI_Recv(data, count, source, -2);
            return MIMPI_SUCCESS;
        }
    }
}

MIMPI_Retcode MIMPI_send_sync_signal_to_parent(int rank, char signal)
{
    if(rank>0)
    {
        return MIMPI_sync_send(signal, (rank-1)/2);
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_send_sync_signal_to_left_child(int rank, int size, char signal)
{
    if(rank*2+1<size)
    {
        return MIMPI_sync_send(signal, rank*2+1);
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_send_sync_signal_to_right_child(int rank, int size, char signal)
{
    if(rank*2+2<size)
    {
        return MIMPI_sync_send(signal, rank*2+2);
    }
    return MIMPI_SUCCESS;
}


MIMPI_Retcode MIMPI_send_sync_signal_to_both_children(int rank, int size, char signal)
{
    pid_t pid1;
    pid_t pid2;
    fflush(stdout);
    ASSERT_SYS_OK(pid1 = fork());
    if(!pid1)
    {
        MIMPI_Retcode status = MIMPI_send_sync_signal_to_left_child(rank,size,signal);
        if (status==MIMPI_SUCCESS)
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
            MIMPI_Retcode status = MIMPI_send_sync_signal_to_right_child(rank,size,signal);
            if (status==MIMPI_SUCCESS)
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
        return MIMPI_SUCCESS;
    }
    else
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
}

void *buffer_messages(void* source_pt)
{
    int source= *((int*)source_pt);
    free(source_pt);
    message_buffers[source]=malloc(sizeof(uint8_t*));
    message_buffers[source][0]=NULL;
    messages_buffered[source]=1;

    char* name = malloc(32*sizeof(char));
    sprintf(name, "MIMPI_channel_from_%d",source);
    int recv_fd=atoi(getenv(name));
    free(name);

    uint8_t* count_bytes=malloc(sizeof(int));
    uint8_t* tag_bytes=malloc(sizeof(int));

    while(true)
    {
        if(chrecv(recv_fd, count_bytes, sizeof(int))==0)
        {
            free(count_bytes);
            free(tag_bytes);
            pthread_mutex_lock(&buffer_mutexes[source]);
            for(int i=0; i<messages_buffered[source]; i++)
            {
                if(message_buffers[source][i]!=NULL)
                {
                    free(message_buffers[source][i]);
                }
            }
            free(message_buffers[source]);
            message_buffers[source]=NULL;
            pthread_mutex_unlock(&(buffer_mutexes[source]));
            return 0;
        }
        else
        {
            chrecv(recv_fd, tag_bytes, sizeof(int));
            int count;
            memcpy(&count, count_bytes, sizeof(int));
            uint8_t* message=malloc(count);
            chrecv(recv_fd, message, count);
            pthread_mutex_lock(&buffer_mutexes[source]);
            int free_space = -1;
            for(int i=0; i<messages_buffered[source]; i++)
            {
                if((message_buffers[i])==NULL)
                {
                    free_space=i;
                    break;
                }
            }
            if(free_space==-1)
            {
                message_buffers[source] = realloc(message_buffers[source], sizeof(message_buffers[source])+sizeof(uint8_t*));
                messages_buffered[source]++;
                free_space=messages_buffered[source]-1;
            }
            message_buffers[source][free_space] = malloc(count+2*sizeof(int));

            for(int i=0; i<sizeof(int); i++)
            {
                message_buffers[source][free_space][i]=count_bytes[i];
                message_buffers[source][free_space][i+sizeof(int)]=tag_bytes[i];
            }
            for(int i=0; i<count; i++)
            {
                message_buffers[source][free_space][i+2*sizeof(int)]=message[i];
            }
            pthread_mutex_unlock(&buffer_mutexes[source]);
            free(message);
            free(tag_bytes);
            free(count_bytes);
        }
    }

    return 0;
}


void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    message_buffers=malloc(size*sizeof(uint8_t**));
    buffer_mutexes=malloc(size*sizeof(pthread_mutex_t));
    buffer_threads=malloc(size*sizeof(pthread_t));
    buffer_conditions=malloc(size*sizeof(pthread_cond_t));
    messages_buffered=malloc(size*sizeof(int));

    for(int i=0; i<size; i++)
    {
        if(i!=rank)
        {
            pthread_mutexattr_t attr;
            ASSERT_ZERO(pthread_mutexattr_init(&attr));
            ASSERT_ZERO(pthread_mutex_init(&buffer_mutexes[i], &attr));
            ASSERT_ZERO(pthread_mutexattr_destroy(&attr));

            int* source_pt = malloc(sizeof(int));
            *source_pt = i;
            pthread_attr_t attr2;
            ASSERT_ZERO(pthread_attr_init(&attr2));
            ASSERT_ZERO(pthread_create(&buffer_threads[i], &attr2, buffer_messages, source_pt));
            ASSERT_ZERO(pthread_attr_destroy(&attr2));

            ASSERT_ZERO(pthread_cond_init(&buffer_conditions[i], NULL));

            messages_buffered[i]=0;
        }
    }

}

void MIMPI_Finalize() {

    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    MIMPI_close_all_program_channels(rank,size);
    
    for(int i=0; i<size; i++)
    {
        if(i!=rank)
        {
            ASSERT_ZERO(pthread_mutex_destroy(&buffer_mutexes[i]));
            ASSERT_ZERO(pthread_cancel(buffer_threads[i]));
        }
    }
    free(buffer_mutexes);
    free(message_buffers);
    
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
    uint8_t *count_bytes=malloc(sizeof(int));
    memcpy(count_bytes, &count, sizeof(int));
    uint8_t *tag_bytes=malloc(sizeof(int));
    memcpy(tag_bytes, &tag, sizeof(int));

    uint8_t *data_to_send=malloc((count+2*sizeof(int)));

    for(int i=0; i<sizeof(int); i++)
    {
        data_to_send[i]=count_bytes[i];
        data_to_send[i+sizeof(int)]=tag_bytes[i];
    }
    free(count_bytes);
    free(tag_bytes);
    for(int i=0; i<count; i++)
    {
        data_to_send[i+2*sizeof(int)]=((uint8_t*)data)[i];
    }

    char* name = malloc(32*sizeof(char));
    sprintf(name, "MIMPI_channel_to_%d",destination);
    int send_fd=atoi(getenv(name));
    free(name);
    
    if(chsend(send_fd, data_to_send, count+2*sizeof(int))==-1)
    {
        free(data_to_send);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else
    {
        free(data_to_send);
        return MIMPI_SUCCESS;
    }
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
    
    pthread_mutex_lock(&buffer_mutexes[source]);
    if(message_buffers[source]==NULL)
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else
    {
        while(true)
        {
            for(int i=0; i<messages_buffered[source]; i++)
            {
                if(message_buffers[source][i]!=NULL)
                {
                    uint8_t *count_bytes=malloc(sizeof(int));
                    uint8_t *tag_bytes=malloc(sizeof(int));
                    for(int j=0; j<sizeof(int); j++)
                    {
                        count_bytes[j]=message_buffers[source][i][j];
                        tag_bytes[j+sizeof(int)]=message_buffers[source][i][j+sizeof(int)];
                    }
                    int mess_count=0;
                    memcpy(&count, count_bytes, sizeof(int));
                    int mess_tag=0;
                    memcpy(&tag, tag_bytes, sizeof(int));
                    if(count==mess_count && tag==mess_tag)
                    {
                        for(int j=0; j<count; j++)
                        {
                            ((uint8_t*)data)[j]=message_buffers[source][i][j+2*sizeof(int)];
                        }
                        pthread_mutex_unlock(&buffer_mutexes[source]);
                        return MIMPI_SUCCESS;
                    }
                }
            }
            pthread_cond_wait(&buffer_conditions[source], &buffer_mutexes[source]);
        }
        
        
    }
}

MIMPI_Retcode MIMPI_Barrier() 
{
    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    char* messch1 = malloc(1*sizeof(char));
    messch1[0] = 'E';                       //EMPTY
    char* messch2 = malloc(1*sizeof(char));
    messch2[0] = 'E';                       //EMPTY
    char* messpar = malloc(1*sizeof(char));
    messpar[0] = 'E';                       //EMPTY

    if(rank*2+1<size)
    {
        if(MIMPI_sync_recv(messch1,rank*2+1)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch1[0] = 'F';
        }
    }
    if(rank*2+2<size)
    {
        if(MIMPI_sync_recv(messch2,rank*2+2)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch2[0] = 'F';
        }
    }

    if(rank==0)
    {
        if(messch1[0]=='F' || messch2[0]=='F')   //some descendant has already finished the MIMPI block
        {
            free(messch1);
            free(messch2);
            free(messpar);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'F');   //FINISHED
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else    //messch1[0]=='B' && messch2[0]=='B'
        {
            free(messch1);
            free(messch2);
            free(messpar);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'B');   //BARRIER
            return MIMPI_SUCCESS;
        }
    }
    else
    {
        if(messch1[0]=='F' || messch2[0]=='F')   //some descendant has already finished the MIMPI block
        {
            free(messch1);
            free(messch2);
            
            MIMPI_Retcode result = MIMPI_send_sync_signal_to_parent(rank, 'F');    //FINISHED
            if(result==MIMPI_ERROR_REMOTE_FINISHED) //parent has finished
            {
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                MIMPI_sync_recv(messpar,(rank-1)/2);
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            
        }
        else    //both children have started MIMPI_Barrier
        {
            free(messch1);
            free(messch2);
            MIMPI_Retcode result = MIMPI_send_sync_signal_to_parent(rank,'B');    //BARRIER
            if(result==MIMPI_ERROR_REMOTE_FINISHED) //parent has finished
            {
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                MIMPI_sync_recv(messpar,(rank-1)/2);
                if(messpar[0]=='F')
                {
                    MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                    free(messpar);
                    return MIMPI_ERROR_REMOTE_FINISHED;
                }
                else    //messpar[0]=='B'
                {
                    MIMPI_send_sync_signal_to_both_children(rank,size,'B');
                    free(messpar);
                    return MIMPI_SUCCESS;
                }
            }

        }
    }
    
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) 
{
    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();
    if(root<0 || root>=size)
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    char* messch1 = malloc(1*sizeof(char));
    messch1[0] = 'E';                       //EMPTY
    char* messch2 = malloc(1*sizeof(char));
    messch2[0] = 'E';                       //EMPTY
    char* messpar = malloc(1*sizeof(char));
    messpar[0] = 'E';                       //EMPTY

    if(rank*2+1<size)
    {
        if(MIMPI_sync_recv(messch1,rank*2+1)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch1[0] = 'F';
        }
    }
    if(rank*2+2<size)
    {
        if(MIMPI_sync_recv(messch2,rank*2+2)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch2[0] = 'F';
        }
    }

    if(rank==0)
    {
        if(messch1[0]=='F' || messch2[0]=='F')   //some descendant has already finished the MIMPI block
        {
            free(messch1);
            free(messch2);
            free(messpar);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'F');   //FINISHED
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else    //(messch1[0]=='R'||messch1[0]=='E') && (messch2[0]=='R'||messch2[0]=='E')
        {
            free(messch1);
            free(messch2);
            free(messpar);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'R');   //BROADCAST

            if(rank==root)
            {
                pid_t pid;
                for(int i=0; i<size; i++)
                {
                    ASSERT_SYS_OK(pid = fork());
                    if(!pid)
                    {
                        if(i!=rank)
                        {
                            MIMPI_Send(data,count,i,-1);
                        }
                        exit(0);
                    }
                }
            }
            else
            {
                MIMPI_Recv(data,count,root,-1);
            }

            return MIMPI_SUCCESS;
        }
    }
    else
    {
        if(messch1[0]=='F' || messch2[0]=='F')   //some descendant has already finished the MIMPI block
        {
            free(messch1);
            free(messch2);
            
            MIMPI_Retcode result = MIMPI_send_sync_signal_to_parent(rank, 'F');    //FINISHED
            if(result==MIMPI_ERROR_REMOTE_FINISHED) //parent has finished
            {
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                MIMPI_sync_recv(messpar,(rank-1)/2);
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            
        }
        else    //both children have started MIMPI_Broadcast
        {
            free(messch1);
            free(messch2);
            MIMPI_Retcode result = MIMPI_send_sync_signal_to_parent(rank, 'R');    //BROADCAST
            if(result==MIMPI_ERROR_REMOTE_FINISHED) //parent has finished
            {
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                MIMPI_sync_recv(messpar,(rank-1)/2);
                if(messpar[0]=='F')
                {
                    MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                    free(messpar);
                    return MIMPI_ERROR_REMOTE_FINISHED;
                }
                else    //messpar[0]=='R'
                {
                    MIMPI_send_sync_signal_to_both_children(rank,size,'R');
                    free(messpar);

                    if(rank==root)
                    {
                        pid_t pid;
                        for(int i=0; i<size; i++)
                        {
                            ASSERT_SYS_OK(pid = fork());
                            if(!pid)
                            {
                                if(i!=rank)
                                {
                                    MIMPI_Send(data,count,i,-1);
                                }
                                exit(0);
                            }
                        }
                    }
                    else
                    {
                        MIMPI_Recv(data,count,root,-1);
                    }

                    return MIMPI_SUCCESS;
                }
            }

        }
    }
    
}


void perform_MIMPI_Op_3(
    uint8_t *child_1_data,
    uint8_t *child_2_data,
    uint8_t *parent_data,
    uint8_t *result,
    int count,
    MIMPI_Op op
)
{
    for(int i=0; i<count; i++)
    {
        if(op==MIMPI_MAX)
        {
            if(child_1_data[i]>child_2_data[i])
            {
                if(parent_data[i]>child_1_data[i])
                {
                    result[i]=parent_data[i];
                }
                else
                {
                    result[i]=child_1_data[i];
                }
            }
            else
            {
                if(parent_data[i]>child_2_data[i])
                {
                    result[i]=parent_data[i];
                }
                else
                {
                    result[i]=child_2_data[i];
                }
            }
        }
        if(op==MIMPI_MIN)
        {
            if(child_1_data[i]<child_2_data[i])
            {
                if(parent_data[i]<child_1_data[i])
                {
                    result[i]=parent_data[i];
                }
                else
                {
                    result[i]=child_1_data[i];
                }
            }
            else
            {
                if(parent_data[i]<child_2_data[i])
                {
                    result[i]=parent_data[i];
                }
                else
                {
                    result[i]=child_2_data[i];
                }
            }
        }
        if(op==MIMPI_SUM)
        {
            result[i]=child_1_data[i]+child_2_data[i]+parent_data[i];
        }
        if(op==MIMPI_PROD)
        {
            result[i]=child_1_data[i]*child_2_data[i]*parent_data[i];
        }
    }
}

void perform_MIMPI_Op_2(
    uint8_t *child_data,
    uint8_t *parent_data,
    uint8_t *result,
    int count,
    MIMPI_Op op
)
{
    for(int i=0; i<count; i++)
    {
        if(op==MIMPI_MAX)
        {
            if(child_data[i]>parent_data[i])
            {
                result[i]=child_data[i];
            }
            else
            {
                result[i]=parent_data[i];
            }
        }
        if(op==MIMPI_MIN)
        {
            if(child_data[i]<parent_data[i])
            {
                result[i]=child_data[i];
            }
            else
            {
                result[i]=parent_data[i];
            }
        }
        if(op==MIMPI_SUM)
        {
            result[i]=child_data[i]+parent_data[i];
        }
        if(op==MIMPI_PROD)
        {
            result[i]=child_data[i]*parent_data[i];
        }
    }
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    char* messch1 = malloc(1*sizeof(char));
    messch1[0] = 'E';                       //EMPTY
    char* messch2 = malloc(1*sizeof(char));
    messch2[0] = 'E';                       //EMPTY
    char* messpar = malloc(1*sizeof(char));
    messpar[0] = 'E';                       //EMPTY

    uint8_t* child_1_data=malloc(count*sizeof(uint8_t));
    uint8_t* child_2_data=malloc(count*sizeof(uint8_t));
    uint8_t* data_to_send=malloc(count*sizeof(uint8_t));

    if(rank*2+2<size)
    {
        if(MIMPI_sync_reduce_recv(messch2,rank*2+2,child_2_data,count)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch2[0] = 'F';
        }
        else if(MIMPI_sync_reduce_recv(messch1,rank*2+1,child_1_data,count)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch1[0] = 'F';
        }
        else if(messch1[0]=='D' && messch2[0]=='D')
        {
            perform_MIMPI_Op_3(child_1_data, child_2_data, (uint8_t*)send_data, data_to_send, count, op);
        }
    }
    else if(rank*2+1<size)
    {
        if(MIMPI_sync_reduce_recv(messch1,rank*2+1,child_1_data,count)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch1[0] = 'F';
        }
        else if(messch1[0]=='D')
        {
            perform_MIMPI_Op_2(child_1_data, (uint8_t*)send_data, data_to_send, count, op);
        }
    }
    else
    {
        for(int i=0; i<count; i++)
        {
            data_to_send[i]=((uint8_t*)send_data)[i];
        }
    }
    free(child_1_data);
    free(child_2_data);
    

    if(rank==0)
    {
        if(messch1[0]=='F' || messch2[0]=='F')   //some descendant has already finished the MIMPI block
        {
            free(messch1);
            free(messch2);
            free(messpar);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'F');   //FINISHED
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else    //messch1[0]=='D' && messch2[0]=='D'
        {
            free(messch1);
            free(messch2);
            free(messpar);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'D');   //REDUCE
            if(root==0)
            {
                for(int i=0; i<count; i++)
                {
                    ((uint8_t*)recv_data)[i]=data_to_send[i];
                }
            }
            else
            {
                MIMPI_Send((void*)data_to_send,count,root,-2);
            }
            free(data_to_send);
            return MIMPI_SUCCESS;
        }
    }
    else
    {
        if(messch1[0]=='F' || messch2[0]=='F')   //some descendant has already finished the MIMPI block
        {
            free(messch1);
            free(messch2);
            free(data_to_send);
            
            MIMPI_Retcode result = MIMPI_send_sync_signal_to_parent(rank, 'F');    //FINISHED
            if(result==MIMPI_ERROR_REMOTE_FINISHED) //parent has finished
            {
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                MIMPI_sync_recv(messpar,(rank-1)/2);
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            
        }
        else    //both children and their descendants have started MIMPI_Reduce
        {
            free(messch1);
            free(messch2);
            MIMPI_Retcode result = MIMPI_sync_reduce_send('D', (rank-1)/2, data_to_send, count);
            free(data_to_send);
            if(result==MIMPI_ERROR_REMOTE_FINISHED) //parent has finished
            {
                MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                free(messpar);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                MIMPI_sync_recv(messpar,(rank-1)/2);
                if(messpar[0]=='F')
                {
                    MIMPI_send_sync_signal_to_both_children(rank,size,'F');
                    free(messpar);
                    return MIMPI_ERROR_REMOTE_FINISHED;
                }
                else    //messpar[0]=='D'
                {
                    MIMPI_send_sync_signal_to_both_children(rank,size,'D');
                    free(messpar);
                    if(rank==root)
                    {
                        MIMPI_Recv(recv_data,count,0,-2);
                    }
                    return MIMPI_SUCCESS;
                }
            }

        }
    }
}

