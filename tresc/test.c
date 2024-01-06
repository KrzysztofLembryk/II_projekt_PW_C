#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "mimpi.h"
#include <stdint.h>

int main(int argc, char *argv[])
{
    //printf("NBR_PROC = %s, MY_PROC_RANK = %s, My pid is %d, my parent's pid is %d\n", getenv("NBR_PROC"), getenv("PROC_RANK"), getpid(), getppid());
    
    printf("MIMPI_WORLD_SIZE = %d, MY_RANK = %d\n", MIMPI_World_size(), MIMPI_World_rank());

    MIMPI_Init(false);

    uint8_t data[4] = {69, 68, 69, 69};
    uint8_t recv_data[4] = {0, 0, 0, 0};

    int my_rank = MIMPI_World_rank();
    if(my_rank == 0)
    {
        MIMPI_Send(data, 4, 1, 1);
    }
    else if(my_rank == 1)
    {
        MIMPI_Recv(recv_data, 4, 0, 1);

        printf("data received: ");
        for(int i = 0; i < 4; i++)
            printf("%hhu ", recv_data[i]);
        printf("\n");
    }

    MIMPI_Finalize();

    return 0;
}