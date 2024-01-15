/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"


struct buffer_node   
{  
    uint8_t *message;   
    struct buffer_node *next;  
};  


struct buffer_node **message_buffers;
pthread_mutex_t *buffer_mutexes;
pthread_t *buffer_threads;
pthread_cond_t *buffer_conditions;
bool *process_left_mimpi;
bool deadlock_detection;
pthread_t *deadlock_threads;
int deadlock_threads_num;
uint8_t ** count_bytes_arr;


void MIMPI_free_message_buffers(int rank)
{
    struct buffer_node *node = message_buffers[rank];
    while(node!=NULL)
    {
        if(node->message!=NULL)
        {
            free(node->message);
            node->message=NULL;
        }
        if(node->next!=NULL)
        {
            struct buffer_node *new_node = node->next;
            free(node);
            node=new_node;
        }
        else
        {
            break;
        }
    }
    free(node);
}


void MIMPI_free_global_variables(bool final)
{
    int size=MIMPI_World_size();
    int rank=MIMPI_World_rank();
    if(final && deadlock_detection)
    {
        for(int i=0; i<size; i++)
        {
            process_left_mimpi[i]=true;
        }
        for(int j=0;j<deadlock_threads_num; j++)
        {
            for(int i=0; i<size; i++)
            {
                if(i!=rank)
                {
                    pthread_cond_signal(&buffer_conditions[i]);
                }
            }
            ASSERT_ZERO(pthread_join(deadlock_threads[j],NULL));
        }
    }
    
    for(int i=0; i<size; i++)
    {
        if(i!=rank)
        {
            if(final)
            {
                pthread_cond_signal(&buffer_conditions[i]);
                ASSERT_ZERO(pthread_join(buffer_threads[i],NULL));
                
            }
            pthread_mutex_destroy(&buffer_mutexes[i]);
            pthread_cond_destroy(&buffer_conditions[i]);
            MIMPI_free_message_buffers(i);
            free(count_bytes_arr[i]);
        }
    }
    free(buffer_mutexes);
    free(message_buffers);
    free(buffer_conditions);
    free(buffer_threads);
    free(process_left_mimpi);
    free(count_bytes_arr);
}




MIMPI_Retcode MIMPI_sync_send(
    char signal,
    int destination
) 
{
    char *signal_arr=malloc(sizeof(char));
    signal_arr[0]=signal;
    MIMPI_Retcode result= MIMPI_Send(signal_arr, 1, destination, -3);
    free(signal_arr);
    return result;
}

MIMPI_Retcode MIMPI_sync_recv(
    char* signal,
    int source
) 
{
    return MIMPI_Recv(signal,1,source,-3);
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
            MIMPI_close_all_program_channels(rank,size);
            MIMPI_free_global_variables(false);
            free(data);
            exit(0);
        }
        else
        {
            MIMPI_close_all_program_channels(rank,size);
            MIMPI_free_global_variables(false);
            free(data);
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
                MIMPI_close_all_program_channels(rank,size);
                MIMPI_free_global_variables(false);
                free(data);
                exit(0);
            }
            else
            {
                MIMPI_close_all_program_channels(rank,size);
                MIMPI_free_global_variables(false);
                free(data);
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
    MIMPI_Retcode result= MIMPI_sync_recv(signal,source);
    if(result==MIMPI_ERROR_REMOTE_FINISHED)
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
            result=MIMPI_Recv(data, count, source, -2);
            return result;
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


MIMPI_Retcode MIMPI_send_sync_signal_to_both_children(int rank, int size, char signal, void* pointer_to_free)
{
    pid_t pid1;
    pid_t pid2;
    fflush(stdout);
    ASSERT_SYS_OK(pid1 = fork());
    if(!pid1)
    {
        MIMPI_Retcode status = MIMPI_send_sync_signal_to_left_child(rank,size,signal);
        if(pointer_to_free!=NULL)
        {
            free(pointer_to_free);
        }
        MIMPI_close_all_program_channels(rank,size);
        MIMPI_free_global_variables(false);
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
            if(pointer_to_free!=NULL)
            {
                free(pointer_to_free);
            }
            MIMPI_close_all_program_channels(rank,size);
            MIMPI_free_global_variables(false);
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

    char* name = malloc(32*sizeof(char));
    sprintf(name, "MIMPI_channel_from_%d",source);
    int recv_fd=atoi(getenv(name));
    free(name);
    

    while(true)
    {
        int result=chrecv(recv_fd, count_bytes_arr[source], sizeof(int));
        if(result<=0)
        {
            pthread_mutex_lock(&buffer_mutexes[source]);
            process_left_mimpi[source]=true;
            pthread_mutex_unlock(&(buffer_mutexes[source]));
            pthread_cond_signal(&(buffer_conditions[source]));
            return 0;
        }
        else
        {
            int count;
            memcpy(&count, count_bytes_arr[source], sizeof(int));
            uint8_t* tag_bytes=malloc(sizeof(int));
            chrecv(recv_fd, tag_bytes, sizeof(int));
            int tag;
            memcpy(&tag, tag_bytes, sizeof(int));
            free(tag_bytes);
            
            
            uint8_t* message=malloc(count);
            if(count<=512)
            {
                chrecv(recv_fd, message, count);
            }
            else
            {
                int count_recvd=0;
                while(count_recvd!=count)
                {
                    if(count-count_recvd<=512)
                    {
                        count_recvd=count_recvd+chrecv(recv_fd,&message[count_recvd],count-count_recvd);
                    }
                    else
                    {
                        count_recvd=count_recvd+chrecv(recv_fd,&message[count_recvd],512);
                    }
                }
            }
            pthread_mutex_lock(&buffer_mutexes[source]);
            struct buffer_node *node;

            if(message_buffers[source]==NULL)
            {
                message_buffers[source]=(struct buffer_node *) malloc(sizeof(struct buffer_node));
                message_buffers[source]->message=NULL;
            }
            if(message_buffers[source]->message==NULL)
            {
                node=message_buffers[source];
            }
            else
            {
                struct buffer_node *last_node=message_buffers[source];
                while(last_node->next!=NULL)
                {
                    last_node=last_node->next;
                }
                last_node->next=(struct buffer_node *) malloc(sizeof(struct buffer_node));
                node=last_node->next;
            }
            node->message = malloc(count+2*sizeof(int));
            node->next=NULL;

            uint8_t* count_bytes_2=malloc(sizeof(int));
            uint8_t* tag_bytes_2=malloc(sizeof(int));
            memcpy(count_bytes_2, &count, sizeof(int));
            memcpy(tag_bytes_2, &tag, sizeof(int));
            for(int i=0; i<sizeof(int); i++)
            {
                node->message[i]=count_bytes_2[i];
                node->message[i+sizeof(int)]=tag_bytes_2[i];
            }
            free(count_bytes_2);
            free(tag_bytes_2);
            for(int i=0; i<count; i++)
            {
                node->message[i+2*sizeof(int)]=message[i];
            }
            pthread_mutex_unlock(&buffer_mutexes[source]);
            pthread_cond_signal(&buffer_conditions[source]);
            free(message);
        }
    }
    return 0;
}


void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    message_buffers=malloc(size*sizeof(struct buffer_node *));
    buffer_mutexes=malloc(size*sizeof(pthread_mutex_t));
    buffer_threads=malloc(size*sizeof(pthread_t));
    buffer_conditions=malloc(size*sizeof(pthread_cond_t));
    process_left_mimpi=malloc(size*sizeof(bool));
    deadlock_threads_num=0;
    count_bytes_arr=malloc(size*sizeof(uint8_t*));

    if(enable_deadlock_detection)
    {
        deadlock_detection=true;
    }
    else
    {
        deadlock_detection=false;
    }

    for(int i=0; i<size; i++)
    {
        if(i!=rank)
        {
            pthread_mutexattr_t attr;
            ASSERT_ZERO(pthread_mutexattr_init(&attr));
            ASSERT_ZERO(pthread_mutex_init(&buffer_mutexes[i], &attr));
            ASSERT_ZERO(pthread_mutexattr_destroy(&attr));

            ASSERT_ZERO(pthread_cond_init(&buffer_conditions[i], NULL));

            process_left_mimpi[i]=false;
            message_buffers[i]=(struct buffer_node*)malloc(sizeof(struct buffer_node));
            //message_buffers[i]->next=(struct buffer_node*)malloc(sizeof(struct buffer_node*));
            message_buffers[i]->next=NULL;
            message_buffers[i]->message=NULL;

            count_bytes_arr[i]=(uint8_t *)malloc(sizeof(int));

            int* source_pt = malloc(sizeof(int));
            *source_pt = i;
            pthread_attr_t attr2;
            ASSERT_ZERO(pthread_attr_init(&attr2));
            ASSERT_ZERO(pthread_create(&buffer_threads[i], &attr2, buffer_messages, source_pt));
            ASSERT_ZERO(pthread_attr_destroy(&attr2));
            //free(source_pt);
        }
    }

}



void MIMPI_Finalize() {
    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();
    MIMPI_close_all_program_channels(rank,size);
    MIMPI_free_global_variables(true);
    fflush(stdout);
    channels_finalize();
}

int MIMPI_World_size() {
    return atoi(getenv("MIMPI_world_size"));
}

int MIMPI_World_rank() {
    return atoi(getenv("MIMPI_world_rank"));
}

void* MIMPI_Recv_R_deadlock_message(void* var_pt);
char MIMPI_Recv_R_or_S_deadlock_message(int source,int expected_count,int expected_tag);

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

    if(deadlock_detection && tag!=-4)   //No recursion!
    {
        char *signal_arr=malloc(sizeof(char)+2*sizeof(int));
        signal_arr[0]='S';
        memcpy(&signal_arr[1], &count, sizeof(int));
        memcpy(&signal_arr[sizeof(int)+1], &tag, sizeof(int));
        MIMPI_Retcode result= MIMPI_Send(signal_arr, sizeof(char)+2*sizeof(int), destination, -4);
        free(signal_arr);
        if(result==MIMPI_ERROR_REMOTE_FINISHED)
        {
            return result;
        }

        int* var_pt=malloc(3*sizeof(int));
        var_pt[0]=destination;
        var_pt[1]=count;
        var_pt[2]=tag;
        deadlock_threads_num=deadlock_threads_num+1;
        deadlock_threads=realloc(deadlock_threads, deadlock_threads_num*sizeof(pthread_t));
        pthread_attr_t attr;
        ASSERT_ZERO(pthread_attr_init(&attr));
        ASSERT_ZERO(pthread_create(&deadlock_threads[deadlock_threads_num-1], &attr, MIMPI_Recv_R_deadlock_message, var_pt));
        ASSERT_ZERO(pthread_attr_destroy(&attr));
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

    int count_sent=0;
    while(count_sent!=(count+2*sizeof(int)))
    {
        if((count+2*sizeof(int))-count_sent<=512)
        {
            int sent=chsend(send_fd,&data_to_send[count_sent],(count+2*sizeof(int))-count_sent);
            if(sent==-1)
            {
                free(data_to_send);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            count_sent=count_sent+sent;
        }
        else
        {
            int sent=chsend(send_fd,&data_to_send[count_sent],512);
            if(sent==-1)
            {
                free(data_to_send);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            count_sent=count_sent+sent;
        }
    }
    free(data_to_send);
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

    if(deadlock_detection)
    {
        char *signal_arr=malloc(sizeof(char)+2*sizeof(int));
        signal_arr[0]='R';
        uint8_t *count_arr=malloc(sizeof(int));
        uint8_t *tag_arr=malloc(sizeof(int));
        memcpy(count_arr, &count, sizeof(int));
        memcpy(tag_arr, &tag, sizeof(int));

        for(int i=0;i<sizeof(int);i++)
        {
            signal_arr[1+i]=count_arr[i];
            signal_arr[1+i+sizeof(int)]=tag_arr[i];
        }
        free(count_arr);
        free(tag_arr);

        MIMPI_Send(signal_arr, sizeof(char)+2*sizeof(int), source, -4);
        free(signal_arr);

        char sync_signal=MIMPI_Recv_R_or_S_deadlock_message(source,count,tag);
        if(sync_signal=='F')
        {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else if(sync_signal=='R')
        {
            pthread_cond_signal(&buffer_conditions[source]);
            return MIMPI_ERROR_DEADLOCK_DETECTED;
        }
        else
        {
            pthread_mutex_lock(&buffer_mutexes[source]);
            int pom=0;
            while(true)
            {
                struct buffer_node *last_node=NULL;
                struct buffer_node *node=message_buffers[source];
                while(node!=NULL && node->message!=NULL)
                {
                    uint8_t *count_bytes=malloc(sizeof(int));
                    uint8_t *tag_bytes=malloc(sizeof(int));
                    for(int j=0; j<sizeof(int); j++)
                    {
                        count_bytes[j]=node->message[j];
                        tag_bytes[j]=node->message[j+sizeof(int)];
                    }
                    int mess_count=0;
                    memcpy(&mess_count, count_bytes, sizeof(int));
                    int mess_tag=0;
                    memcpy(&mess_tag, tag_bytes, sizeof(int));
                    free(count_bytes);
                    free(tag_bytes);
                    if(count==mess_count && (tag==mess_tag || tag==MIMPI_ANY_TAG))
                    {
                        for(int j=0; j<count; j++)
                        {
                            ((uint8_t*)data)[j]=node->message[j+2*sizeof(int)];
                        }
                        free(node->message);
                        
                        if(last_node==NULL)
                        {
                            if(node->next!=NULL)
                            {
                                message_buffers[source]=node->next;
                                free(node);
                            }
                            else
                            {
                                node->message=NULL;
                            }
                        }
                        else
                        {
                            last_node->next=node->next;
                            free(node);
                        }
                        

                        pthread_mutex_unlock(&buffer_mutexes[source]);
                        return MIMPI_SUCCESS;
                    }
                    last_node=node;
                    node=node->next;
                }
                if(process_left_mimpi[source]==true)
                {
                    if(pom==0)
                    {
                        pom++;
                        continue;
                    }
                    pthread_mutex_unlock(&buffer_mutexes[source]);
                    return MIMPI_ERROR_REMOTE_FINISHED;
                }
                ASSERT_SYS_OK(pthread_cond_wait(&buffer_conditions[source], &buffer_mutexes[source]));
            }

        }

    }
    else
    {
        pthread_mutex_lock(&buffer_mutexes[source]);
        int pom=0;
        while(true)
        {
            struct buffer_node *last_node=NULL;
            struct buffer_node *node=message_buffers[source];
            while(node!=NULL && node->message!=NULL)
            {
                uint8_t *count_bytes=malloc(sizeof(int));
                uint8_t *tag_bytes=malloc(sizeof(int));
                for(int j=0; j<sizeof(int); j++)
                {
                    count_bytes[j]=node->message[j];
                    tag_bytes[j]=node->message[j+sizeof(int)];
                }
                int mess_count=0;
                memcpy(&mess_count, count_bytes, sizeof(int));
                int mess_tag=0;
                memcpy(&mess_tag, tag_bytes, sizeof(int));
                free(count_bytes);
                free(tag_bytes);
                if(count==mess_count && (tag==mess_tag || tag==MIMPI_ANY_TAG))
                {
                    for(int j=0; j<count; j++)
                    {
                        ((uint8_t*)data)[j]=node->message[j+2*sizeof(int)];
                    }
                    free(node->message);
                    
                    if(last_node==NULL)
                    {
                        if(node->next!=NULL)
                        {
                            message_buffers[source]=node->next;
                        }
                        else
                        {
                            node->message=NULL;
                        }
                    }
                    else
                    {
                        last_node->next=node->next;
                        free(node);
                    }
                    

                    pthread_mutex_unlock(&buffer_mutexes[source]);
                    return MIMPI_SUCCESS;
                }
                last_node=node;
                node=node->next;
            }
            if(process_left_mimpi[source]==true)
            {
                if(pom==0)
                {
                    pom++;
                    continue;
                }
                pthread_mutex_unlock(&buffer_mutexes[source]);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            ASSERT_SYS_OK(pthread_cond_wait(&buffer_conditions[source], &buffer_mutexes[source]));
        }
    }

}


void* MIMPI_Recv_R_deadlock_message(
    void* var_pt
) {
    int source= ((int*)var_pt)[0];
    int expected_count=((int*)var_pt)[1];
    int expected_tag=((int*)var_pt)[2];
    free(var_pt);


    if (source == MIMPI_World_rank())
    {
        return 0;
    }
    if (source < 0 || source >= MIMPI_World_size())
    {
        return 0;
    }
    pthread_mutex_lock(&buffer_mutexes[source]);
    int pom=0;
    while(true)
    {
        struct buffer_node *last_node=NULL;
        struct buffer_node *node=message_buffers[source];
        while(node!=NULL && node->message!=NULL)
        {
            uint8_t *count_bytes=malloc(sizeof(int));
            uint8_t *tag_bytes=malloc(sizeof(int));
            for(int j=0; j<sizeof(int); j++)
            {
                count_bytes[j]=node->message[j];
                tag_bytes[j]=node->message[j+sizeof(int)];
            }
            int mess_count=0;
            memcpy(&mess_count, count_bytes, sizeof(int));
            int mess_tag=0;
            memcpy(&mess_tag, tag_bytes, sizeof(int));
            free(count_bytes);
            free(tag_bytes);
            if(mess_count==sizeof(char)+2*sizeof(int) && mess_tag==-4 && node->message[2*sizeof(int)]=='R')
            {
                uint8_t *count_bytes=malloc(sizeof(int));
                uint8_t *tag_bytes=malloc(sizeof(int));
                for(int j=0; j<sizeof(int); j++)
                {
                    count_bytes[j]=node->message[j+1+2*sizeof(int)];
                    tag_bytes[j]=node->message[j+1+3*sizeof(int)];
                }
                int r_count=0;
                memcpy(&r_count, count_bytes, sizeof(int));
                int r_tag=0;
                memcpy(&r_tag, tag_bytes, sizeof(int));
                free(count_bytes);
                free(tag_bytes);

                if(r_tag==expected_tag && r_count==expected_count)
                {
                    free(node->message);
                    
                    if(last_node==NULL)
                    {
                        if(node->next!=NULL)
                        {
                            message_buffers[source]=node->next;
                        }
                        else
                        {
                            node->message=NULL;
                        }
                    }
                    else
                    {
                        last_node->next=node->next;
                        free(node);
                    }
                    

                    pthread_mutex_unlock(&buffer_mutexes[source]);
                    return 0;
                }
            }
            last_node=node;
            node=node->next;
        }
        if(process_left_mimpi[source]==true)
        {
            if(pom==0)
            {
                pom++;
                continue;
            }
            pthread_mutex_unlock(&buffer_mutexes[source]);
            return 0;
        }
        ASSERT_SYS_OK(pthread_cond_wait(&buffer_conditions[source], &buffer_mutexes[source]));
    }
}


char MIMPI_Recv_R_or_S_deadlock_message(
    int source,
    int expected_count,
    int expected_tag
) {

    pthread_mutex_lock(&buffer_mutexes[source]);
    int pom=0;
    while(true)
    {   
        for(int i=0;i<2;i++)
        {
            struct buffer_node *last_node=NULL;
            struct buffer_node *node=message_buffers[source];
            while(node!=NULL && node->message!=NULL)
            {

                uint8_t *count_bytes=malloc(sizeof(int));
                uint8_t *tag_bytes=malloc(sizeof(int));
                for(int j=0; j<sizeof(int); j++)
                {
                    count_bytes[j]=node->message[j];
                    tag_bytes[j]=node->message[j+sizeof(int)];
                }
                int mess_count=0;
                memcpy(&mess_count, count_bytes, sizeof(int));
                int mess_tag=0;
                memcpy(&mess_tag, tag_bytes, sizeof(int));
                free(count_bytes);
                free(tag_bytes);
                if(mess_count==sizeof(char)+2*sizeof(int) && mess_tag==-4 && ((node->message[2*sizeof(int)]=='R' && i==1) || (node->message[2*sizeof(int)]=='S')))
                {
                    uint8_t *count_bytes=malloc(sizeof(int));
                    uint8_t *tag_bytes=malloc(sizeof(int));
                    for(int j=0; j<sizeof(int); j++)
                    {
                        count_bytes[j]=node->message[2*sizeof(int)+1+j];
                        tag_bytes[j]=node->message[3*sizeof(int)+1+j];
                    }
                    int r_count=0;
                    memcpy(&r_count, count_bytes, sizeof(int));
                    int r_tag=0;
                    memcpy(&r_tag, tag_bytes, sizeof(int));
                    free(count_bytes);
                    free(tag_bytes);

                    if((r_tag==expected_tag && r_count==expected_count && node->message[2*sizeof(int)]=='S') || node->message[2*sizeof(int)]=='R')
                    {
                        char result=node->message[2*sizeof(int)];
                        free(node->message);
                        
                        if(last_node==NULL)
                        {
                            if(node->next!=NULL)
                            {
                                message_buffers[source]=node->next;
                            }
                            else
                            {
                                node->message=NULL;
                            }
                        }
                        else
                        {
                            last_node->next=node->next;
                            free(node);
                        }
                        

                        pthread_mutex_unlock(&buffer_mutexes[source]);
                        return result;
                    }
                }
                last_node=node;
                node=node->next;
            }
        }
        if(process_left_mimpi[source]==true)
        {
            if(pom==0)
            {
                pom++;
                continue;
            }
            pthread_mutex_unlock(&buffer_mutexes[source]);
            return 'F';
        }
        ASSERT_SYS_OK(pthread_cond_wait(&buffer_conditions[source], &buffer_mutexes[source]));
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
            MIMPI_send_sync_signal_to_both_children(rank, size, 'F', NULL);   //FINISHED
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else    //messch1[0]=='B' && messch2[0]=='B'
        {
            free(messch1);
            free(messch2);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'B', NULL);   //BARRIER
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
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                char* messpar = malloc(1*sizeof(char));
                messpar[0] = 'E';                       //EMPTY
                MIMPI_sync_recv(messpar,(rank-1)/2);
                free(messpar);
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
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
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                char* messpar = malloc(1*sizeof(char));
                messpar[0] = 'E';                       //EMPTY
                MIMPI_Retcode result = MIMPI_sync_recv(messpar,(rank-1)/2);
                if(messpar[0]=='F' || result==MIMPI_ERROR_REMOTE_FINISHED)
                {
                    free(messpar);
                    MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                    return MIMPI_ERROR_REMOTE_FINISHED;
                }
                else    //messpar[0]=='B'
                {
                    free(messpar);
                    MIMPI_send_sync_signal_to_both_children(rank,size,'B', NULL);
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
            MIMPI_send_sync_signal_to_both_children(rank, size, 'F', NULL);   //FINISHED
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else    //(messch1[0]=='R'||messch1[0]=='E') && (messch2[0]=='R'||messch2[0]=='E')
        {
            free(messch1);
            free(messch2);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'R', NULL);   //BROADCAST

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
                        MIMPI_close_all_program_channels(rank,size);
                        MIMPI_free_global_variables(false);
                        exit(0);
                    }
                }
                for(int i=0; i<size-1; i++)
                {
                    ASSERT_SYS_OK(wait(NULL));
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
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                char* messpar = malloc(1*sizeof(char));
                messpar[0] = 'E';                       //EMPTY
                MIMPI_sync_recv(messpar,(rank-1)/2);
                free(messpar);
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
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
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                char* messpar = malloc(1*sizeof(char));
                messpar[0] = 'E';                       //EMPTY
                MIMPI_sync_recv(messpar,(rank-1)/2);
                if(messpar[0]=='F')
                {
                    free(messpar);
                    MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                    return MIMPI_ERROR_REMOTE_FINISHED;
                }
                else    //messpar[0]=='R'
                {
                    free(messpar);
                    MIMPI_send_sync_signal_to_both_children(rank,size,'R', NULL);

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
                                MIMPI_close_all_program_channels(rank,size);
                                MIMPI_free_global_variables(false);
                                exit(0);
                            }
                        }
                        for(int i=0; i<size-1; i++)
                        {
                            ASSERT_SYS_OK(wait(NULL));
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
    

    uint8_t* child_1_data=malloc((count+1)*sizeof(uint8_t));
    uint8_t* child_2_data=malloc((count+1)*sizeof(uint8_t));
    uint8_t* data_to_send=malloc((count+1)*sizeof(uint8_t));


    if(rank*2+2<size)
    {
        if(MIMPI_sync_reduce_recv(messch2,rank*2+2,child_2_data,count)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch2[0] = 'F';
        }
        if(MIMPI_sync_reduce_recv(messch1,rank*2+1,child_1_data,count)==MIMPI_ERROR_REMOTE_FINISHED)
        {
            messch1[0] = 'F';
        }
        if(messch1[0]=='D' && messch2[0]=='D')
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
            free(data_to_send);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'F', NULL);   //FINISHED
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        else    //messch1[0]=='D' && messch2[0]=='D'
        {
            free(messch1);
            free(messch2);
            MIMPI_send_sync_signal_to_both_children(rank, size, 'D', data_to_send);   //REDUCE
            if(root==0)
            {
                for(int i=0; i<count; i++)
                {
                    ((uint8_t*)recv_data)[i]=data_to_send[i];
                }
            }
            else
            {
                MIMPI_Send(data_to_send,count,root,-2);
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
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                char *messpar = malloc(1*sizeof(char));
                MIMPI_sync_recv(messpar,(rank-1)/2);
                free(messpar);
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
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
                MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            else
            {
                char* messpar = malloc(1*sizeof(char));
                messpar[0] = 'E';                       //EMPTY
                MIMPI_Retcode result = MIMPI_sync_recv(messpar,(rank-1)/2);
                if(messpar[0]=='F' || result==MIMPI_ERROR_REMOTE_FINISHED)
                {
                    free(messpar);
                    MIMPI_send_sync_signal_to_both_children(rank,size,'F', NULL);
                    return MIMPI_ERROR_REMOTE_FINISHED;
                }
                else    //messpar[0]=='D'
                {
                    free(messpar);
                    MIMPI_send_sync_signal_to_both_children(rank,size,'D', NULL);
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