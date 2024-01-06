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

void data_init(Data *data)
{
    data->MY_STDIN = OFFSET + MIMPI_World_rank() * 2;
    data->MY_STDOUT = data->MY_STDIN + 1;
}

/**
 * We close all writing descrpt to other processes, we only need to read what
 * they wrote. We write only to our pipe, so that each process knows from whom
 * it receives data.
*/ 
void close_redundant_dscrpt(Data *data)
{
    int nbr_proc = MIMPI_World_size();
    int i = 0;
    int curr_read_dscrpt = OFFSET;

    while(i < nbr_proc)
    {
        if(curr_read_dscrpt == data->MY_STDIN)
        {
            printf("closing my stdin: %d\n", curr_read_dscrpt);
            close(curr_read_dscrpt);
        }
        else
        {
            printf("closing: %d\n", curr_read_dscrpt + 1);
            close(curr_read_dscrpt + 1);
        }
            
        
        i++;
        curr_read_dscrpt += 2;
    }
}

void MIMPI_Init(bool enable_deadlock_detection)
{
    channels_init();
    data_init(&mimpi_data);
    close_redundant_dscrpt(&mimpi_data);

    // TODO
}

void MIMPI_Finalize()
{
    // TODO

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

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag)
{

    // TODO
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag)
{
    // TODO
}

MIMPI_Retcode MIMPI_Barrier()
{
    // TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root)
{
    // TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root)
{
    // TODO
}