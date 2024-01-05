/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"

int main(int argc, char* argv[]) {

    // Minimal nbr of args is 3: programme name, nbr of copies, path
    if(argc < 3)
        return -1;

    int nbr_of_copies_to_run = atoi(argv[1]);
    const char *path = argv[2];
    int other_args = atoi(argv[3]);

    printf("name: %s, copies: %d, path: %s, other: %d\n", argv[0], nbr_of_copies_to_run, path, other_args);

    char **rest_of_args;
    
        int n = argc - 2 + 1; // null terminated array we need
        printf("rest_args ssize: %d\n", n);
        rest_of_args = calloc(n, sizeof(char*));

        memcpy(rest_of_args, argv + 2, n * sizeof(*rest_of_args));
        //for(int i = 0; i < n; i++)
        //    rest_of_args[i] = argv[i + 3];

        printf("many args!!!, restArgs[0]: %s, rest[1]: %s\n\n", 
            rest_of_args[0], rest_of_args[1]);
        
    
    

    for(int i = 0; i < nbr_of_copies_to_run; i++)
    {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);

        if(!pid)
        {
            ASSERT_SYS_OK(execvp(*rest_of_args, rest_of_args));   
        }

    }

    if (nbr_of_copies_to_run != 0)
        ASSERT_SYS_OK(wait(NULL));

    free(rest_of_args);

    return 0;
    //TODO
}