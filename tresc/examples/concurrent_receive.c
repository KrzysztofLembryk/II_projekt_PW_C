#include <stdio.h>

#include "../mimpi.h"
#include "../mimpi_common.h"
void print_Retcode(MIMPI_Retcode code)
{
    if (code == MIMPI_SUCCESS)
        printf("MIMPI_SUCCESS");
    if (code == MIMPI_ERROR_REMOTE_FINISHED)
        printf("MIMPI_ERROR_REMOTE_FINISHED");
}


int main() {
    MIMPI_Init(0);
    MIMPI_Retcode ret_code;
    if (MIMPI_World_rank() != 0) {
        int a[10];
        for (int i = 0; i < 10000; i++) {
            ret_code = MIMPI_Send(a, 10 * sizeof(int), 0, 1);
            //print_Retcode(ret_code); printf("\n");
        }
    }

    MIMPI_Barrier();
    printf("Done!\n");
    MIMPI_Finalize();
}