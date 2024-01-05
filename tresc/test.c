#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "mimpi.h"

int main(int argc, char *argv[])
{
    //printf("NBR_PROC = %s, MY_PROC_RANK = %s, My pid is %d, my parent's pid is %d\n", getenv("NBR_PROC"), getenv("PROC_RANK"), getpid(), getppid());
    
    printf("MIMPI_WORLD_SIZE = %d, MY_RANK = %d\n", MIMPI_World_size(), MIMPI_World_rank());

    return 0;
}