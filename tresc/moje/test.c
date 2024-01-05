#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>

int main(int argc, char *argv[])
{
    if(argv[2] == NULL)
        printf("Dziala - null na koncu\n");
        
    printf("My pid is %d, my parent's pid is %d\n", getpid(), getppid());
    printf("name: %s, nbr args: %d, other: %s\n", argv[0], argc, argv[1]);

    return 0;
}