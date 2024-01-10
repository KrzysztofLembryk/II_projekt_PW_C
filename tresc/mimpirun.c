/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"

/**
 * In parent process, for each child process we create 2 * nbr_proc pipes, one
 * for writing and one for reading for each present process (we need to do it
 * since we cannot create pipes in child processes, and when we want to read we
 * need to close writing dscrptr, and closed dscrptr cannot be reopened).
 * We will use descriptors starting from 20. It will look like this:
 * p1 ---- p1 : deleted
 * p1 ----> p2
 * p1 ----> p3
 * p1 ----> p4
 * p2 ----> p1
 * p2 ---- p2 : deleted
 *    ....
 * When i.e. p2 wants to receive sth from p1, it knows that p1 will be
 * transfering data, so it finds p1 pipes, and goes to pipe that has its idx
 * and waits for input. When p1 wants to send sth to p2 it goes to pipes it has
 * and finds pipe p1 ----> p2 and sends data. So p1 needs to close all reading
 * ends of its pipes because it will only send stuff via them, and close all
 * write ends of other processes' pipes.
 *
 * Formula for dscrptr for proc of rank 0 for each child: 20 + i * 2 * nbr_proc,
 *  where 20 is OFFSET.
 */
void prepare_pipes(int nbr_of_proc)
{
    const int READ_DSCR = 0;
    const int WRITE_DSCR = 1;
    int start_dscrpt;
    int file_dscrpt[2];

    for (int i = 0; i < nbr_of_proc; i++)
    {
        start_dscrpt = OFFSET + i * 2 * nbr_of_proc;

        // ASSERT_SYS_OK(channel(file_dscrpt));

        for (int j = 0; j < 2 * nbr_of_proc; j++)
        {
            //printf("Descriptor: %d\n", start_dscrpt + j);

            if (j % 2 == 0)
            {
                ASSERT_SYS_OK(channel(file_dscrpt));
                dup2(file_dscrpt[READ_DSCR], start_dscrpt + j);
            }
            else
            {
                dup2(file_dscrpt[WRITE_DSCR], start_dscrpt + j);
                close(file_dscrpt[READ_DSCR]);
                close(file_dscrpt[WRITE_DSCR]);
            }
        }
        //printf("---------------\n");
    }
}

void close_dscrpt_from_dup2(int proc_rank, int nbr_of_proc)
{

    int start_dscrpt = OFFSET + proc_rank * 2 * nbr_of_proc;

    for (int j = 0; j < 2 * nbr_of_proc; j++)
    {
        close(start_dscrpt + j);
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
    // char *NBR_PROC = "MIMPI_NBR_PROC";
    putenv(NBR_PROC);
    setenv(NBR_PROC, argv[1], 1);

    // char *PROC_RANK = "MIMPI_PROC_RANK";
    putenv(PROC_RANK);

    // We want all argv arguments starting from argv[2] where name of programme
    // to exec is stored.
    char **rest_of_args = &argv[2];
    char idx_str[12];
    int nbr_of_proc = atoi(argv[1]);
    //printf("nbr of proc: %d\n", nbr_of_proc);
    const int REPLACE = 1;

    prepare_pipes(nbr_of_proc);

    for (int i = 0; i < nbr_of_proc; i++)
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

    // We close all dscrpts that we got from dup2.
    for(int i = 0; i < nbr_of_proc; i++)
    {
        close_dscrpt_from_dup2(i, nbr_of_proc);
    }

    // We wait for every child.
    for (int i = 0; i < nbr_of_proc; i++)
        ASSERT_SYS_OK(wait(NULL));

    return 0;
}