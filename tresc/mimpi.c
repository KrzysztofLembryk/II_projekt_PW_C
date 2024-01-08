/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
//#include <semaphore.h>
#include  <pthread.h>

typedef struct Data
{
    int nbr_of_proc;
    int *proc_left_MIMPI;
    // Mutex will guard adding data to queue list and modifying variables. 
    pthread_mutex_t mutex;
    // On parent_cond parent process will wait if it doesn't find proper recv
    // on queue list.
    pthread_cond_t parent_cond;


} Data;

Data mimpi_data;

void data_init(Data *data)
{
    data->nbr_of_proc = MIMPI_World_size();
    data->proc_left_MIMPI = calloc(data->nbr_of_proc, sizeof(int));
}

/**
 * We close all writing descrpt to other processes, we only need to read what
 * they wrote. We write only to our pipe, so that each process knows from whom
 * it receives data.
 */
void close_redundant_dscrpt()
{
    int nbr_proc = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    int curr_proc = 0;
    int curr_read_dscrpt = OFFSET;
    int curr_rank;
    // printf("\nMIMPI INIT\n");
    // printf("\n-----MY RANK: %d-----\n", my_rank);

    while (curr_proc < nbr_proc)
    {
        // printf("curr_proc: %d\n", curr_proc);
        curr_rank = 0;

        if (curr_proc == my_rank)
        {
            //printf("curr_proc = my_rank = %d\n", my_rank);
            //  When curr_proc is equal to our rank, we close only reading ends
            //  of our pipes, since we will use them to write, and others will
            //  read from them.
            while (curr_rank < nbr_proc)
            {
                if (curr_proc == curr_rank)
                {
                    //printf("closing %d, %d\n",
                    //     curr_read_dscrpt, curr_read_dscrpt + 1);
                    close(curr_read_dscrpt);
                    close(curr_read_dscrpt + 1);
                }
                else
                {
                    //printf("closing read %d\n", curr_read_dscrpt);
                    close(curr_read_dscrpt);
                }
                curr_rank++;
                curr_read_dscrpt += 2;
            }
        }
        else // curr_proc != my_rank
        {
            //printf("curr_proc != my_rank\n");
            while (curr_rank < nbr_proc)
            {
                // If curr_proc is not us, we close all pipes except our pipe
                // in curr_proc. In our pipe we close read end of pipe.
                if (my_rank != curr_rank)
                {
                    //printf("closing %d, %d\n",
                    //     curr_read_dscrpt, curr_read_dscrpt + 1);
                    close(curr_read_dscrpt);
                    close(curr_read_dscrpt + 1);
                }
                else
                {
                    //printf("closing write %d\n", curr_read_dscrpt + 1);
                    close(curr_read_dscrpt + 1);
                }
                curr_rank++;
                curr_read_dscrpt += 2;
            }
        }

        curr_proc++;
        //printf("---------------\n");
    }
}

void close_all_left_dscrptrs()
{
    int nbr_proc = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    int curr_read_dscrpt;
    int i;
    //printf("MIMPI FINALIZE\n");
    for (int curr_proc = 0; curr_proc < nbr_proc; curr_proc++)
    {
        curr_read_dscrpt = OFFSET + curr_proc * 2 * nbr_proc;

        if (curr_proc == my_rank)
        {
            i = 0;
            //printf("curr_proc == my_rank\n");
            while (i < nbr_proc)
            {
                // In my_rank process we left opened all write ends of pipes, 
                // so we need to close them now.
                if (i != my_rank)
                {
                    //printf("closing read: %d\n", curr_read_dscrpt + 1);
                    close(curr_read_dscrpt + 1);
                }
                    
                
                i++;
                curr_read_dscrpt += 2;
            }
        }
        else // curr_proc != my_rank
        {
            // We need to close only reading ends of pipes at our indexes, cause
            // all other ends are closed.
            curr_read_dscrpt += 2 * my_rank;
            //printf("closing %d\n", curr_read_dscrpt);

            close(curr_read_dscrpt);
        }
    }
}

void MIMPI_Init(bool enable_deadlock_detection)
{
    channels_init();
    close_redundant_dscrpt();
}

void MIMPI_Finalize()
{
    close_all_left_dscrptrs();
    channels_finalize();
}

int MIMPI_World_size()
{
    static bool init = false;
    static int nbr_of_proc;

    if (!init)
    {
        nbr_of_proc = atoi(getenv(NBR_PROC));
        init = true;
    }
    return nbr_of_proc;
}

int MIMPI_World_rank()
{
    static bool init = false;
    static int my_rank;

    if (!init)
    {
        my_rank = atoi(getenv(PROC_RANK));
        init = true;
    }
    return my_rank;
}

/**
 * 
 */
MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag)
{
    if (destination == MIMPI_World_rank())
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    if(destination >= MIMPI_World_size() || destination < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;

    // We want array of bytes, so we need to cast void ptr to unint8_t.
    uint8_t *data_to_send = (uint8_t *)data;

    printf("data to send: ");
    for (int i = 0; i < count; i++)
        printf("%hhu ", data_to_send[i]);
    printf("\n");

    int my_rank = MIMPI_World_rank();
    int nbr_proc = MIMPI_World_size();

    // We calc first our descryptor, allowed dscrp for our use are in [20, 1023]
    // Each process has continuous disjoint part of [20, 1023] for its 
    // descryptors, each such part is of size = 2*nbr_proc (we need read and 
    // write dscrptrs).
    int MY_STARTING_DSCRPT = OFFSET + my_rank * 2 * nbr_proc;

    // Read dscrpt are firs ones, write are second ones, so MY_STARTING_DSCRPT
    // is a first read dscrpt, thus each time we find read dscrpt and to get
    // write dscrpt we need to add 1.
    int MY_STDOUT = MY_STARTING_DSCRPT + 2 * destination + 1;

    int send_ret_code = chsend(MY_STDOUT, &tag, 1);
    send_ret_code = chsend(MY_STDOUT, &count, 1);
    send_ret_code = chsend(MY_STDOUT, data_to_send, count);

    printf("chsend ret code %d\n", send_ret_code);

    if(send_ret_code == -1)
        return MIMPI_ERROR_REMOTE_FINISHED;

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag)
{
    if (source == MIMPI_World_rank())
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    if(source >= MIMPI_World_size() || source < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;

    int my_rank = MIMPI_World_rank();
    int nbr_proc = MIMPI_World_size();

    // We read data from source process' pipe, so we need to find where it 
    // starts, and then our read dscrpt in found block of source proc dscrptrs.
    int SRC_STARTING_DSCRPT = OFFSET + source * 2 * nbr_proc;
    int MY_READ_DSCRPT_IN_SRC = SRC_STARTING_DSCRPT + 2 * my_rank;

    int receive_ret_code = chrecv(MY_READ_DSCRPT_IN_SRC, data, count);

    if(receive_ret_code == 0)
        return MIMPI_ERROR_REMOTE_FINISHED;

    // uint8_t *received_data = (uint8_t*)data;

    return MIMPI_SUCCESS;
    // TODO
}

MIMPI_Retcode MIMPI_Barrier()
{
    // TODO
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root)
{
    // TODO
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root)
{
    // TODO
    return MIMPI_SUCCESS;
}