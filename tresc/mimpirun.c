/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"

/**
 * In parent process, for each child process we create two pipes, one for 
 * writing and one for reading (we need to do it since we cannot create pipes 
 * in child processes, and when we want to read we need to close writing 
 * dscrptr, and closed dscrptr cannot be reopened).
 * We will use descriptors starting from 20. So child with idx = 0 will have
 * 20, 21, 22, 23 descriptors for itself. 
 * Where:
 * 20, 21 - for reading other children writng, 
 * 22, 23 - for writing to other children,
 * so child0 will need to close its 21 and 22 descriptors. Whereas other 
 * children will need to close 20 and 23. Formula for calculating first 
 * descriptor for each child is: 20 + i * 4, where 20 is OFFSET.
*/
void prepare_pipes(int nbr_od_proc)
{
    const int READ_DSCR = 0;
    const int WRITE_DSCR = 1;
    int start_dscrpt;
    int file_dscrpt[2];

    for(int i = 0; i < nbr_od_proc; i++)
    {
        start_dscrpt = OFFSET + i * 4;

        for(int j = 0; j < 4; j++)
        {
            printf("Descriptor: %d\n", start_dscrpt + j);
            if(j == 0 || j == 2)
                ASSERT_SYS_OK(channel(file_dscrpt));
            
            if(j % 2 == 0)
                dup2(file_dscrpt[READ_DSCR], start_dscrpt + j);
            else
            {
                dup2(file_dscrpt[WRITE_DSCR], start_dscrpt + j);
                close(file_dscrpt[READ_DSCR]);
                close(file_dscrpt[WRITE_DSCR]);
            }  
        }
        printf("---------------\n");
    }
}

int main(int argc, char *argv[])
{
    // Minimal nbr of args is 3: programme name, nbr of copies, path
    if (argc < 3)
        return -1;

    // ----------ENVIRONMENT PREPARATIONS----------

    // We make env var to store process indexes, and env var to store how many
    // processes we run. 
    //char *NBR_PROC = "MIMPI_NBR_PROC";
    putenv(NBR_PROC);
    setenv(NBR_PROC, argv[1], 1);

    //char *PROC_RANK = "MIMPI_PROC_RANK";
    putenv(PROC_RANK);
    
    // We want all argv arguments starting from argv[2] where name of programme
    // to exec is stored.
    char **rest_of_args = &argv[2];
    char idx_str[12];
    int nbr_of_copies_to_run = atoi(argv[1]);
    const int REPLACE = 1;

    prepare_pipes(nbr_of_copies_to_run);

    for (int i = 0; i < nbr_of_copies_to_run; i++)
    {
        // After fork environment variables of parent are copied to child,
        // so after fork, changes we make in these vars won't be seen in parent
        // env.
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);

        if (!pid)
        {
            // We set our env var to our rank which is i.
            sprintf(idx_str, "%d", i);
            setenv(PROC_RANK, idx_str, REPLACE);

            ASSERT_SYS_OK(execvp(*rest_of_args, rest_of_args));
        }    
    }

    // We wait for every child.
    for (int i = 0; i < nbr_of_copies_to_run; i++)
        ASSERT_SYS_OK(wait(NULL));

    return 0;
    // TODO
}