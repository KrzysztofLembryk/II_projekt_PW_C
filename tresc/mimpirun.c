/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"

int main(int argc, char *argv[])
{
    // Minimal nbr of args is 3: programme name, nbr of copies, path
    if (argc < 3)
        return -1;

    int nbr_of_copies_to_run = atoi(argv[1]);
    //const char *path = argv[2];

    // We need null terminated array, so size needs to be + 1
    //int rest_size = argc - 2 + 1;
    //char **rest_of_args = calloc(rest_size, sizeof(char *));
    
    // We want all argv arguments starting from argv[2] where name of programme
    // to exec is stored.
    char **rest_of_args = &argv[2];
    //memcpy(rest_of_args, argv + 2, rest_size * sizeof(*rest_of_args));

    for (int i = 0; i < nbr_of_copies_to_run; i++)
    {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);

        if (!pid)
            ASSERT_SYS_OK(execvp(*rest_of_args, rest_of_args));
    }

    // We wait for every child.
    for (int i = 0; i < nbr_of_copies_to_run; i++)
        ASSERT_SYS_OK(wait(NULL));

    //free(rest_of_args);

    return 0;
    // TODO
}