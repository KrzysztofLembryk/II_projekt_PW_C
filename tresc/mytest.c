// #include <assert.h>
// #include <stdbool.h>
// #include <stdnoreturn.h>

// #include <unistd.h>
// #include <stdlib.h>
// #include <stdio.h>
// #include <string.h>
// #include <sys/types.h>
// #include <sys/wait.h>
// #include <stdint.h>
#include "mimpi_common.h"
#include "mimpi.h"
#include <errno.h>
#include <time.h>

int m_sleep(long msec)
{
    struct timespec ts;
    int res;

    if (msec < 0)
    {
        errno = EINVAL;
        return -1;
    }
    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do
    {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);

    return res;
}


int main(int argc, char *argv[])
{
    //printf("NBR_PROC = %s, MY_PROC_RANK = %s, My pid is %d, my parent's pid is %d\n", getenv("NBR_PROC"), getenv("PROC_RANK"), getpid(), getppid());
    
    //printf("MIMPI_WORLD_SIZE = %d, MY_RANK = %d\n", MIMPI_World_size(),MIMPI_World_rank());
    
    int my_rank = MIMPI_World_rank();

    //if(my_rank == 0)
    //    sleep(2);
    printf("proc %d mimpi init\n", my_rank);
    MIMPI_Init(false);
    
    
    uint8_t data[4] = {69, 68, 69, 69};
    uint8_t recv_data[4] = {0, 0, 0, 0};

    
    if(my_rank == 0)
    {
        // printf("proc %d sleeping\n", my_rank);
        // sleep(2);
        uint8_t val_to_Send = 69;
        MIMPI_Retcode ret_send = MIMPI_Send(&val_to_Send, sizeof(val_to_Send), 1, 1);
        if(ret_send != MIMPI_SUCCESS)
            printf("I wanted to send but no-one waited\n");
        else
            printf("MIMPI SUCCESS - message sent\n");
    }
    else if(my_rank == 1)
    {
        MIMPI_Recv(recv_data, 1, 0, 1);
        //m_sleep(1);
        printf("--------Ended receiving--------\n\n");
        printf("data received: ");
        for(int i = 0; i < 1; i++)
           printf("%hhu ", recv_data[i]);
        printf("\n");
    }

    MIMPI_Finalize();

    return 0;
}