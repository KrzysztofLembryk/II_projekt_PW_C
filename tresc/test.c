#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>

int main(int argc, char *argv[])
{
    printf("NBR_PROC = %s, MY_PROC_RANK = %s, My pid is %d, my parent's pid is %d\n", getenv("NBR_PROC"), getenv("PROC_RANK"), getpid(), getppid());
    
    return 0;
}