/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    //TODO
}

void MIMPI_Finalize() {
    //TODO

    channels_finalize();
}

int MIMPI_World_size() {
    static bool init = false;
    static int nbr_of_proc;
    if(!init)
    {
        nbr_of_proc = atoi(getenv("NBR_PROC"));
        init = true;
    }
    return nbr_of_proc;
}

int MIMPI_World_rank() {
    static bool init = false;
    static int my_rank;

    if(!init)
    {
        my_rank = atoi(getenv("PROC_RANK"));
        init = true;
    }
    return my_rank;
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    //TODO
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    //TODO
}

MIMPI_Retcode MIMPI_Barrier() {
    //TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    //TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    //TODO
}