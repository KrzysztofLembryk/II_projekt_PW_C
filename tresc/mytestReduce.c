#include "mimpi_common.h"
#include "mimpi.h"
#include <errno.h>
#include <time.h>

int main(int argc, char *argv[])
{

    int my_rank = MIMPI_World_rank();

    // if(my_rank == 0)
    //     sleep(2);
    printf("proc %d mimpi init\n", my_rank);
    MIMPI_Init(false);

    int data_size = 4;
    uint8_t send_data[4] = {1, 1, 1, 1};
    uint8_t recv_data[4] = {0, 0, 0, 0};
    MIMPI_Op const op = MIMPI_SUM;
    uint8_t results[] = {3, 3, 3, 3};
    int root = 1;

    if(MIMPI_Reduce(send_data, recv_data, data_size, op, root) != MIMPI_SUCCESS)
    {
        printf("proc %d REDUCE NOT SUCCESFUL\n", my_rank);
    }
    if (my_rank == root)
    {
        
        for (int k = 0; k < data_size; ++k)
        {
            if (recv_data[k] != results[k])
            {
                printf("MISMATCH got %i, should be %i\n", recv_data[k], 
                results[k]);
                // fprintf(stderr, "MISMATCH: op_idx %i, root %i, k=%i: expected %i, got %i.\n", i, root, k, results[i], recv_data[k]);
            }
            // test_assert(recv_data[k] == result);
        }
    }
}