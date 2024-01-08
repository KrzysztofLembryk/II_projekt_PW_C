/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <semaphore.h>
#include <pthread.h>

typedef struct QElem
{
    int proc_rank;
    int tag;
    int count;
    uint8_t *data;
    struct QElem *prev;
    struct QElem *next;
} QElem;

QElem *QElem_make_new(int _rank, int _tag, int _count, uint8_t *_data)
{
    QElem *elem = (QElem *)malloc(sizeof(QElem));
    elem->proc_rank = _rank;
    elem->tag = _tag;
    elem->count = _count;
    elem->data = _data;
    elem->prev = NULL;
    elem->next = NULL;
    return elem;
}

bool QElem_is_the_same(QElem *el, int rank, int tag, int count)
{
    return (el->proc_rank == rank) && (el->tag == tag || tag == 0) &&
           (el->count == count);
}

typedef struct QueueList
{
    QElem *front;
    QElem *end;

} QueueList;

void queue_init(QueueList *q)
{
    q->front = NULL;
    q->end = NULL;
}

void queue_push_back(QueueList *q, QElem *new_elem)
{
    if (q->front == NULL)
    {
        q->front = new_elem;
        q->end = new_elem;
    }
    else
    {
        q->end->prev = new_elem;
        new_elem->next = q->end;
        q->end = new_elem;
    }
}

/**
 * Function finds elem that has attributes equal to given rank, tag and count,
 * removes this elem from queue and returns ptr to it. If such elem doesnt exist
 * it returns NULL.
 */
QElem *queue_find_elem(QueueList *q, int rank, int tag, int count)
{
    QElem *curr_elem = q->front;

    while (curr_elem != NULL)
    {
        if (QElem_is_the_same(curr_elem, rank, tag, count))
        {
            if (curr_elem->next != NULL)
            {
                curr_elem->next->prev = curr_elem->prev;

                if (curr_elem->prev != NULL)
                    curr_elem->prev->next = curr_elem->next;
                else
                {
                    // If curr_elem->prev = NULL this means curr_elem is END.
                    q->end = q->end->next;
                }
                return curr_elem;
            }
            else
            {
                // If curr_elem->next = NULL this means curr_elem is FRONT.
                q->front = q->front->prev;
                if (q->front != NULL)
                    q->front->next = NULL;
                else
                    q->end = NULL;

                return curr_elem;
            }
        }
        curr_elem = curr_elem->prev;
    }

    // We didnt find sought elem so we return NULL.
    return NULL;
}

typedef struct Handler
{
    int nbr_of_proc;

    // proc_left_MIMPI - array to check when to ret MIMPI_ERROR_REMOTE_FINISHED
    // or if our parent process is in mimpi finalize.
    int *proc_left_MIMPI;

    // wanted_* parameters say which data parent proc wants to read.
    int wanted_rank;
    int wanted_count;
    int wanted_tag;

    // For each process that can write to us we allocate queue for its data.
    QueueList *tab_of_queues;

    // We create one thread for each proc that can send data to us.
    // So we need an array to store our threads.
    pthread_t *reading_threads;

    // Mutex will guard adding data to queue list and modifying variables.
    pthread_mutex_t mutex;

    // On parent_cond parent process will wait if it doesn't find proper recv
    // on queue list.
    pthread_cond_t parent_cond;

} Handler;

Handler mimpi_handler;

void handler_init(Handler *handler)
{
    handler->nbr_of_proc = MIMPI_World_size();
    handler->proc_left_MIMPI = calloc(handler->nbr_of_proc, sizeof(int));
    handler->wanted_count = -1;
    handler->wanted_rank = -1;
    handler->wanted_tag = -1;

    handler->tab_of_queues = calloc(handler->nbr_of_proc, sizeof(QueueList));

    for (int i = 0; i < handler->nbr_of_proc; i++)
    {
        queue_init(&(handler->tab_of_queues[i]));
    }

    handler->reading_threads = calloc(handler->nbr_of_proc, sizeof(pthread_t));

    ASSERT_ZERO(pthread_mutex_init(&(handler->mutex), NULL));
    ASSERT_ZERO(pthread_cond_init(&handler->parent_cond, NULL));
}

void inform_that_proc_left_MIMPI_mutex(int proc_rank)
{
    // Mimpi_send always sends tag as first elem, so when we get ret
    // code 0 from first read we know that pipe is closed and process is
    // no longer in MIMPI section. So we need to change status of
    // source_rank proc in proc_left_MIMPI to true.
    pthread_mutex_lock(&mimpi_handler.mutex);

    mimpi_handler.proc_left_MIMPI[proc_rank] = 1;

    // If our parent process waits for data from proc of source_rank
    // we need to wake him up, then parent checks if sought data is
    // present in queue, if not it should return ERROR_REMOTE_FINISHED
    if (mimpi_handler.wanted_rank == proc_rank)
        pthread_cond_signal(&mimpi_handler.parent_cond);

    pthread_mutex_unlock(&mimpi_handler.mutex);
}

void add_received_data_to_MIMPI_mutex(QElem *elem, int source_rank)
{
    // Now we get mutex and need to push_back (tag, count, source, data)
    // to our queue list. Then we check if added data by us is data that
    // parent wants.
    ASSERT_ZERO(pthread_mutex_lock(&mimpi_handler.mutex));

    queue_push_back(&(mimpi_handler.tab_of_queues[source_rank]), elem);

    // If elem we added is the one that parent looks for we signal the
    // parent and give him critical section.
    if (QElem_is_the_same(elem, mimpi_handler.wanted_rank,
                          mimpi_handler.wanted_tag, mimpi_handler.wanted_count))
        ASSERT_ZERO(pthread_cond_signal(&mimpi_handler.parent_cond));

    ASSERT_ZERO(pthread_mutex_unlock(&mimpi_handler.mutex));
}

void* read_what_other_proc_send(void *arg)
{
    // source_rank says from who we receive data, we want value of int ptr arg
    // so first we cast it to int* and then we get value by * operator
    int source_rank = *((int *)arg);
    int nbr_of_proc = MIMPI_World_size();
    int parent_rank = MIMPI_World_rank();

    // We calculate descrptr from which we will read.
    int SRC_STARTING_DSCRPT = OFFSET + source_rank * 2 * nbr_of_proc;
    int MY_STDIN = SRC_STARTING_DSCRPT + 2 * parent_rank;

    // We receive tag, then count then data.
    int tag;
    int count;
    uint8_t *received_data;

    int ret_code;
    int read_bytes = 0;

    // We will read from source, then pushback read data to queue, and again
    // read from source till first read returns 0.
    while (true)
    {
        // We don't need to acquire mutex, since we only read from array.
        // If our parent process invoked MIMPI_finalize we dont want to read
        // data any longer, so we check and break.
        if (mimpi_handler.proc_left_MIMPI[parent_rank])
            break;

        ret_code = chrecv(MY_STDIN, &tag, sizeof(int));
        if (ret_code == 0)
        {
            // inform func acquires mutex, so its safe.
            inform_that_proc_left_MIMPI_mutex(source_rank);
            printf("thread : Reading from %d proc closed\n", source_rank);
            break;
        }

        // After receiving count = how many bytes we will read in chrecv
        // we need to allocate that many bytes in our received_data variable
        // so that we could store all read data and in future parent process
        // could copy this data.
        chrecv(MY_STDIN, &count, sizeof(int));

        received_data = calloc(count, sizeof(uint8_t));

        // Count = how many bytes we will read from pipe,
        // count might be greater than pipes buffor so we need to read from
        // buffor till read_bytes are equal to our count.
        while (read_bytes < count)
        {
            // received_data is a pointer to the 0 elem of our array, so in
            // order not to overwrite already saved data we need to save new
            // data starting from first free place.
            // We can only read PIPE_READ_SIZE bytes atomically from pipe, so we
            // either read 512 bytes or less than 512 bytes in one read.
            if((count - read_bytes) > PIPE_READ_SIZE)
                read_bytes += chrecv(MY_STDIN, received_data + read_bytes,
                                 PIPE_READ_SIZE);
            else
                read_bytes += chrecv(MY_STDIN, received_data + read_bytes,
                                 count - read_bytes);
        }
        read_bytes = 0;
        QElem *elem = QElem_make_new(source_rank, tag, count, received_data);

        // add_received_data acquires mutex, so its safe.
        add_received_data_to_MIMPI_mutex(elem, source_rank);
    }
}

/**
 * We close all writing descrpt to other processes, we only need to read what
 * they wrote. We write only to our pipe, so that each process knows from whom
 * it receives data.
 */
void close_redundant_dscrpt()
{
    int nbr_proc = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    int curr_proc = 0;
    int curr_read_dscrpt = OFFSET;
    int curr_rank;
    // printf("\nMIMPI INIT\n");
    // printf("\n-----MY RANK: %d-----\n", my_rank);

    while (curr_proc < nbr_proc)
    {
        // printf("curr_proc: %d\n", curr_proc);
        curr_rank = 0;

        if (curr_proc == my_rank)
        {
            // printf("curr_proc = my_rank = %d\n", my_rank);
            //   When curr_proc is equal to our rank, we close only reading ends
            //   of our pipes, since we will use them to write, and others will
            //   read from them.
            while (curr_rank < nbr_proc)
            {
                if (curr_proc == curr_rank)
                {
                    // printf("closing %d, %d\n",
                    //      curr_read_dscrpt, curr_read_dscrpt + 1);
                    close(curr_read_dscrpt);
                    close(curr_read_dscrpt + 1);
                }
                else
                {
                    // printf("closing read %d\n", curr_read_dscrpt);
                    close(curr_read_dscrpt);
                }
                curr_rank++;
                curr_read_dscrpt += 2;
            }
        }
        else // curr_proc != my_rank
        {
            // printf("curr_proc != my_rank\n");
            while (curr_rank < nbr_proc)
            {
                // If curr_proc is not us, we close all pipes except our pipe
                // in curr_proc. In our pipe we close read end of pipe.
                if (my_rank != curr_rank)
                {
                    // printf("closing %d, %d\n",
                    //      curr_read_dscrpt, curr_read_dscrpt + 1);
                    close(curr_read_dscrpt);
                    close(curr_read_dscrpt + 1);
                }
                else
                {
                    // printf("closing write %d\n", curr_read_dscrpt + 1);
                    close(curr_read_dscrpt + 1);
                }
                curr_rank++;
                curr_read_dscrpt += 2;
            }
        }

        curr_proc++;
        // printf("---------------\n");
    }
}

void close_all_left_dscrptrs()
{
    int nbr_proc = MIMPI_World_size();
    int my_rank = MIMPI_World_rank();
    int curr_read_dscrpt;
    int i;
    // printf("MIMPI FINALIZE\n");
    for (int curr_proc = 0; curr_proc < nbr_proc; curr_proc++)
    {
        curr_read_dscrpt = OFFSET + curr_proc * 2 * nbr_proc;

        if (curr_proc == my_rank)
        {
            i = 0;
            // printf("curr_proc == my_rank\n");
            while (i < nbr_proc)
            {
                // In my_rank process we left opened all write ends of pipes,
                // so we need to close them now.
                if (i != my_rank)
                {
                    // printf("closing read: %d\n", curr_read_dscrpt + 1);
                    close(curr_read_dscrpt + 1);
                }

                i++;
                curr_read_dscrpt += 2;
            }
        }
        else // curr_proc != my_rank
        {
            // We need to close only reading ends of pipes at our indexes, cause
            // all other ends are closed.
            curr_read_dscrpt += 2 * my_rank;
            // printf("closing %d\n", curr_read_dscrpt);

            close(curr_read_dscrpt);
        }
    }
}

void MIMPI_Init(bool enable_deadlock_detection)
{
    channels_init();
    close_redundant_dscrpt();
    handler_init(&mimpi_handler);
    
    int my_rank = MIMPI_World_rank();
    for(int i = 0; i < mimpi_handler.nbr_of_proc; i++)
    {
        if(i != my_rank)
        {
            ASSERT_ZERO(pthread_create(&mimpi_handler.reading_threads[i], NULL, read_what_other_proc_send, (void*)&my_rank));    
        }                      
    }
}

void MIMPI_Finalize()
{
    close_all_left_dscrptrs();
    channels_finalize();
}

int MIMPI_World_size()
{
    static bool init = false;
    static int nbr_of_proc;

    if (!init)
    {
        nbr_of_proc = atoi(getenv(NBR_PROC));
        init = true;
    }
    return nbr_of_proc;
}

int MIMPI_World_rank()
{
    static bool init = false;
    static int my_rank;

    if (!init)
    {
        my_rank = atoi(getenv(PROC_RANK));
        init = true;
    }
    return my_rank;
}

/**
 *
 */
MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag)
{
    if (destination == MIMPI_World_rank())
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    if (destination >= MIMPI_World_size() || destination < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;

    // We want array of bytes, so we need to cast void ptr to unint8_t.
    uint8_t *data_to_send = (uint8_t *)data;

    printf("data to send: ");
    for (int i = 0; i < count; i++)
        printf("%hhu ", data_to_send[i]);
    printf("\n");

    int my_rank = MIMPI_World_rank();
    int nbr_proc = MIMPI_World_size();

    // We calc first our descryptor, allowed dscrp for our use are in [20, 1023]
    // Each process has continuous disjoint part of [20, 1023] for its
    // descryptors, each such part is of size = 2*nbr_proc (we need read and
    // write dscrptrs).
    int MY_STARTING_DSCRPT = OFFSET + my_rank * 2 * nbr_proc;

    // Read dscrpt are firs ones, write are second ones, so MY_STARTING_DSCRPT
    // is a first read dscrpt, thus each time we find read dscrpt and to get
    // write dscrpt we need to add 1.
    int MY_STDOUT = MY_STARTING_DSCRPT + 2 * destination + 1;

    int send_ret_code = chsend(MY_STDOUT, &tag, sizeof(int));
    send_ret_code = chsend(MY_STDOUT, &count, sizeof(int));
    send_ret_code = chsend(MY_STDOUT, data_to_send, count);

    printf("chsend ret code %d\n", send_ret_code);

    if (send_ret_code == -1)
        return MIMPI_ERROR_REMOTE_FINISHED;

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag)
{
    if (source == MIMPI_World_rank())
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    if (source >= MIMPI_World_size() || source < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;

    int my_rank = MIMPI_World_rank();
    int nbr_proc = MIMPI_World_size();

    // We read data from source process' pipe, so we need to find where it
    // starts, and then our read dscrpt in found block of source proc dscrptrs.
    int SRC_STARTING_DSCRPT = OFFSET + source * 2 * nbr_proc;
    int MY_READ_DSCRPT_IN_SRC = SRC_STARTING_DSCRPT + 2 * my_rank;

    int receive_ret_code = chrecv(MY_READ_DSCRPT_IN_SRC, data, count);

    if (receive_ret_code == 0)
        return MIMPI_ERROR_REMOTE_FINISHED;

    // uint8_t *received_data = (uint8_t*)data;

    return MIMPI_SUCCESS;
    // TODO
}

MIMPI_Retcode MIMPI_Barrier()
{
    // TODO
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root)
{
    // TODO
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root)
{
    // TODO
    return MIMPI_SUCCESS;
}