/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

typedef struct Data
{
    int MY_STDIN;
    int MY_STDOUT;

} Data;

Data mimpi_data;

// void data_init(Data *data)
// {
//     data->MY_STDIN = OFFSET + MIMPI_World_rank() * 2;
//     data->MY_STDOUT = data->MY_STDIN + 1;
// }

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

    // printf("my rank: %d\n", my_rank);

    while (curr_proc < nbr_proc)
    {
        // printf("curr_proc: %d\n", curr_proc);
        curr_rank = 0;

        if (curr_proc == my_rank)
        {
            // printf("curr_proc = my_rank, %d = %d\n", my_rank, curr_proc);
            //  When curr_proc is equal to our rank, we close only reading ends
            //  of our pipes, since we will use them to write, and others will
            //  read from them.
            while (curr_rank < nbr_proc)
            {
                if (curr_proc == curr_rank)
                {
                    // printf("closing %d, %d\n",
                    //     curr_read_dscrpt, curr_read_dscrpt + 1);
                    close(curr_read_dscrpt);
                    close(curr_read_dscrpt + 1);
                }
                else
                {
                    // printf("closing read %d\n", curr_read_dscrpt);
                    close(curr_read_dscrpt);
                }
                curr_rank++;
                curr_read_dscrpt += 2;
            }
        }
        else
        {
            while (curr_rank < nbr_proc)
            {
                // If curr_proc is not us, we close all pipes except our pipe
                // in curr_proc.
                if (my_rank != curr_rank)
                {
                    // printf("closing %d, %d\n",
                    //     curr_read_dscrpt, curr_read_dscrpt + 1);
                    close(curr_read_dscrpt);
                    close(curr_read_dscrpt + 1);
                }
                else
                {
                    // printf("closing write %d\n", curr_read_dscrpt + 1);
                    close(curr_read_dscrpt + 1);
                }
                curr_rank++;
                curr_read_dscrpt += 2;
            }
        }

        curr_proc++;
        // printf("---------------\n\n");
    }
}

void close_all_left_dscrptrs()
{
    int nbr_proc = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    // printf("close all left dscrpt, my rank: %d\n", my_rank);
    int curr_read_dscrpt;
    int i;

    for (int curr_proc = 0; curr_proc < nbr_proc; curr_proc++)
    {
        curr_read_dscrpt = OFFSET + curr_proc * 2 * nbr_proc;
        // printf("curr dscrpt: %d\n", curr_read_dscrpt);

        if (curr_proc == my_rank)
        {
            // printf("curr_proc = my_rank = %d\n", curr_proc);
            i = 0;
            while (i < nbr_proc)
            {
                // In my_rank process we left opened all write ends of pipes, so
                // we need to close them now.
                if (curr_proc != i)
                {
                    // printf("closing : %d\n", curr_read_dscrpt + 1);
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
            // printf("closing read dscrpt : %d\n", curr_read_dscrpt);
            close(curr_read_dscrpt);
        }
    }
}

void MIMPI_Init(bool enable_deadlock_detection)
{
    channels_init();
    close_redundant_dscrpt();

    // TODO
}

void MIMPI_Finalize()
{
    // TODO
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
 * We see data array of count bytes, so we need to cast void ptr to unint8_t
 */
MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag)
{
    if (destination == MIMPI_World_rank())
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;

    uint8_t *data_to_send = (uint8_t *)data;

    printf("data to send: ");
    for (int i = 0; i < count; i++)
        printf("%hhu ", data_to_send[i]);
    printf("\n");

    int my_rank = MIMPI_World_rank();
    int nbr_proc = MIMPI_World_size();

    int MY_STARTING_DSCRPT = OFFSET + my_rank * 2 * nbr_proc;
    int MY_STDOUT = MY_STARTING_DSCRPT + 2 * destination + 1;

    chsend(MY_STDOUT, data_to_send, count);
    // TODO
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

    int my_rank = MIMPI_World_rank();
    int nbr_proc = MIMPI_World_size();

    int SRC_STARTING_DSCRPT = OFFSET + source * 2 * nbr_proc;
    int DEST_READ_DSCRPT = SRC_STARTING_DSCRPT + 2 * my_rank;
    
    chrecv(DEST_READ_DSCRPT, data, count);

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