/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"

int main(int argc, char *argv[])
{
    // Minimal nbr of args is 3: programme name, nbr of copies, path
    if (argc < 3)
        return -1;

    // We make env var to store process indexes, and env var to store how many
    // processes we run. 
    char *NBR_PROC = "NBR_PROC";
    putenv(NBR_PROC);
    setenv(NBR_PROC, argv[1], 1);

    char *PROC_RANK = "PROC_RANK";
    putenv(PROC_RANK);
    
    // We want all argv arguments starting from argv[2] where name of programme
    // to exec is stored.
    char **rest_of_args = &argv[2];
    char idx_str[12];
    int nbr_of_copies_to_run = atoi(argv[1]);

    for (int i = 0; i < nbr_of_copies_to_run; i++)
    {
        // After fork environment variables of parent are copied to child,
        // so after fork, changes we make in these vars won't be seen in parent
        // env.
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);

        if (!pid)
        {
            sprintf(idx_str, "%d", i);
            setenv(PROC_RANK, idx_str, 1);
            ASSERT_SYS_OK(execvp(*rest_of_args, rest_of_args));
        }    
    }

    // We wait for every child.
    for (int i = 0; i < nbr_of_copies_to_run; i++)
        ASSERT_SYS_OK(wait(NULL));

    return 0;
    // TODO
}