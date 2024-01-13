/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <semaphore.h>
#include <pthread.h>
#include <time.h>
#include <sys/select.h>

#include <errno.h>

int msleep(long msec)
{
    struct timespec ts;
    int res;

    if (msec < 0)
    {
        errno = EINVAL;
        return -1;
    }

    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do
    {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);

    return res;
}

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

void QElem_destruct(QElem *elem)
{
    free(elem->data);
    free(elem);
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

void queue_destruct(QueueList *q)
{
    QElem *curr_elem = q->front;
    QElem *prev;
    while (curr_elem != NULL)
    {
        prev = curr_elem->prev;
        QElem_destruct(curr_elem);
        curr_elem = prev;
    }

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
            // Found first occurence of wanted data, so we remove it from lst.
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

    bool is_sought_data_present;
    bool parent_wake_up;

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
    handler->wanted_count = COUNT_NOT_WANTED;
    handler->wanted_rank = RANK_NOT_WANTED;
    handler->wanted_tag = TAG_NOT_WANTED;
    handler->is_sought_data_present = false;
    handler->parent_wake_up = false;
    handler->tab_of_queues = calloc(handler->nbr_of_proc, sizeof(QueueList));

    for (int i = 0; i < handler->nbr_of_proc; i++)
    {
        queue_init(&(handler->tab_of_queues[i]));
    }

    handler->reading_threads = calloc(handler->nbr_of_proc, sizeof(pthread_t));

    ASSERT_ZERO(pthread_mutex_init(&(handler->mutex), NULL));
    ASSERT_ZERO(pthread_cond_init(&handler->parent_cond, NULL));
}

void handler_destruct(Handler *handler)
{
    free(handler->proc_left_MIMPI);
    free(handler->reading_threads);

    for (int i = 0; i < mimpi_handler.nbr_of_proc; i++)
    {
        queue_destruct(&mimpi_handler.tab_of_queues[i]);
    }

    free(mimpi_handler.tab_of_queues);

    ASSERT_ZERO(pthread_mutex_destroy(&handler->mutex));
    ASSERT_ZERO(pthread_cond_destroy(&handler->parent_cond));
}

void inform_that_SRCproc_left_MIMPI_mutex(int proc_rank)
{
    // Mimpi_send always sends tag as first elem, so when we get ret
    // code 0 from first read we know that pipe is closed and process is
    // no longer in MIMPI section. So we need to change status of
    // source_rank proc in proc_left_MIMPI to true.
    pthread_mutex_lock(&mimpi_handler.mutex);
    // printf("thread informing that proc source left MIMPI\n");
    mimpi_handler.proc_left_MIMPI[proc_rank] = 1;

    // If our parent process waits for data from proc of source_rank
    // we need to wake him up, then parent checks if sought data is
    // present in queue, if not it should return ERROR_REMOTE_FINISHED
    if (mimpi_handler.wanted_rank == proc_rank)
    {
        mimpi_handler.parent_wake_up = true;
        pthread_cond_signal(&mimpi_handler.parent_cond);
    }

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
    {
        // printf("Added elem to queue is sought by parent, I'm waking him up\n");
        mimpi_handler.is_sought_data_present = true;
        mimpi_handler.parent_wake_up = true;
        ASSERT_ZERO(pthread_cond_signal(&mimpi_handler.parent_cond));
    }

    ASSERT_ZERO(pthread_mutex_unlock(&mimpi_handler.mutex));
}

void *read_what_other_proc_send(void *arg)
{
    // source_rank says from who we receive data, we want value of int ptr arg
    // so first we cast it to int* and then we get value by * operator
    int *source_rank_ptr = arg;
    int source_rank = *source_rank_ptr;

    // Parent thread allocated memory for that arg, we need to free it.
    free(source_rank_ptr);

    int nbr_of_proc = MIMPI_World_size();
    int parent_rank = MIMPI_World_rank();

    // We calculate descrptr from which we will read.
    int SRC_STARTING_DSCRPT = OFFSET + source_rank * 2 * nbr_of_proc;
    int PARENT_DSCRPT = OFFSET + parent_rank * 2 * nbr_of_proc;

    int MY_STDIN = SRC_STARTING_DSCRPT + 2 * parent_rank;
    int MY_STDIN_FROM_PARENT = PARENT_DSCRPT + 2 * parent_rank;
    // printf("My parent: %d, his stdin: %d\n", parent_rank, MY_STDIN_FROM_PARENT);
    //  Needed for fd_set.
    int bigger_stdin =
        (MY_STDIN > MY_STDIN_FROM_PARENT) ? MY_STDIN : MY_STDIN_FROM_PARENT;

    // We receive tag, then count then data.
    int tag;
    int count;
    uint8_t *received_data;

    // int ret_code;

    // This set will be used to wait for either tag from src proc or parent proc
    fd_set dscrpt_set_src_and_parent;

    while (true)
    {
        // We don't need to acquire mutex, since we only read from array.
        // If our parent process invoked MIMPI_finalize we dont want to read
        // data any longer, so we check and break.
        if (mimpi_handler.proc_left_MIMPI[parent_rank])
            break;

        // We init our fd_set with two dscrpt that we want to read from.
        // LNIUX MAN:
        // Upon return, each of the file descriptor sets is
        // modified in place to indicate which file descriptors are
        // currently "ready".  Thus, if using select() within a loop, the
        // sets must be reinitialized before each call.
        FD_ZERO(&dscrpt_set_src_and_parent);
        FD_SET(MY_STDIN, &dscrpt_set_src_and_parent);
        FD_SET(MY_STDIN_FROM_PARENT, &dscrpt_set_src_and_parent);
        // printf("thread waiting on select\n");
        select(bigger_stdin + 1, &dscrpt_set_src_and_parent, NULL, NULL, NULL);

        // Now we check which dscrpt is still in set, if not MY_STDIN it means
        // that someone wrote sth to MY_STDIN.
        if (FD_ISSET(MY_STDIN, &dscrpt_set_src_and_parent))
        {

            // We get message from source proc.
            chrecv(MY_STDIN, &tag, sizeof(tag));
        }
        else
        {
            // printf("thread received tag from parent proc\n");
            chrecv(MY_STDIN_FROM_PARENT, &tag, sizeof(tag));
        }

        // printf("Got tag %d\n", tag);
        //  We check the message we got, if its not one of the two below we can
        //  read more data from pipe.
        if (tag == PARENT_PROC_IN_FINALIZE)
        {
            // printf("thread breaking : parent proc in finalize\n");
            break;
        }
        else if (tag == SRC_PROC_IN_FINALIZE)
        {
            // Message from src proc that it left mIMPI, so we need to inform
            // that it left and if needed wake up my parent process.
            // printf("thread breaking : src proc left mimpi, informing parent\n");
            inform_that_SRCproc_left_MIMPI_mutex(source_rank);
            break;
        }

        // After receiving count = how many bytes we will read in chrecv
        // we need to allocate that many bytes in our received_data variable
        // so that we could store all read data and in future parent process
        // could copy this data.
        chrecv(MY_STDIN, &count, sizeof(count));

        received_data = calloc(count, sizeof(uint8_t));
        int read_bytes = 0;
        // Count might be greater than pipes buffor so we need to read from
        // buffor till read_bytes are equal to our count.
        while (read_bytes < count)
        {
            // received_data is a pointer to the 0 elem of our array, so in
            // order not to overwrite already saved data we need to save new
            // data starting from first free place.
            // We can only read PIPE_READ_SIZE bytes atomically from pipe, so we
            // either read 512 bytes or less than 512 bytes in one read.
            if ((count - read_bytes) > PIPE_READ_SIZE)
                read_bytes += chrecv(MY_STDIN, received_data + read_bytes,
                                     PIPE_READ_SIZE);
            else
                read_bytes += chrecv(MY_STDIN, received_data + read_bytes,
                                     count - read_bytes);
        }
        // read_bytes = 0;

        QElem *elem = QElem_make_new(source_rank, tag, count, received_data);

        // add_received_data acquires mutex, adds data to queue and if added
        // data is a wanted data it wakes up parent proc.
        add_received_data_to_MIMPI_mutex(elem, source_rank);
    }
    return NULL;
}

//----------MIMPI_INIT----------

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
                    // We dont close our reading and writing dscrpt since we
                    // will use them to communicate with our threads.
                    // close(curr_read_dscrpt);
                    // close(curr_read_dscrpt + 1);
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

void MIMPI_Init(bool enable_deadlock_detection)
{
    channels_init();
    close_redundant_dscrpt();
    handler_init(&mimpi_handler);

    int my_rank = MIMPI_World_rank();

    // if(my_rank == 0)
    //     sleep(1);

    for (int proc_rank = 0; proc_rank < mimpi_handler.nbr_of_proc; proc_rank++)
    {
        if (proc_rank != my_rank)
        {
            // POSSIBLE MEMORY LEAK!
            int *rank_for_thread = (int *)malloc(sizeof(int));
            *rank_for_thread = proc_rank;

            // printf("Creating thread for reading from proc %d\n", proc_rank);
            ASSERT_ZERO(pthread_create(
                &mimpi_handler.reading_threads[proc_rank], NULL, read_what_other_proc_send, (void *)rank_for_thread));
        }
    }
}

//----------MIMPI_FINALIZE----------

void send_finalize_to_all_threads()
{
    int my_rank = MIMPI_World_rank();
    int nbr_proc = MIMPI_World_size();
    int MY_STARTING_DSCRPT = OFFSET + my_rank * 2 * nbr_proc;
    int MY_STDOUT = MY_STARTING_DSCRPT + 2 * my_rank + 1;
    // Read dscrpt are firs ones, write are second ones, so MY_STARTING_DSCRPT
    // is a first read dscrpt, thus each time we find read dscrpt and to get
    // write dscrpt we need to add 1.
    int tag = PARENT_PROC_IN_FINALIZE;
    for (int i = 0; i < nbr_proc; i++)
    {
        if (i != my_rank)
        {
            if(!mimpi_handler.proc_left_MIMPI[i])
                chsend(MY_STDOUT, &tag, sizeof(tag));
        }
            
    }
}

// We need to informa other processes that we leave mimpi block, so that their
// threads can end and inform their parent processes.
void send_finalize_to_all_other_proc()
{
    int my_rank = MIMPI_World_rank();
    int nbr_proc = MIMPI_World_size();
    int MY_STARTING_DSCRPT = OFFSET + my_rank * 2 * nbr_proc;
    int MY_STDOUT;
    int tag = SRC_PROC_IN_FINALIZE;

    for (int proc_rank = 0; proc_rank < nbr_proc; proc_rank++)
    {
        if (my_rank != proc_rank)
        {
            if(!mimpi_handler.proc_left_MIMPI[proc_rank])
            {
                MY_STDOUT = MY_STARTING_DSCRPT + 2 * proc_rank + 1;
                chsend(MY_STDOUT, &tag, sizeof(tag));
            }
        }
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
                else
                {
                    close(curr_read_dscrpt);
                    close(curr_read_dscrpt + 1);
                }

                i++;
                curr_read_dscrpt += 2;
            }
        }
        else // curr_proc != my_rank
        {
            // We need to close only reading ends of pipes from other process'
            // at our indexes, cause all other ends are already closed.
            curr_read_dscrpt += 2 * my_rank;
            // printf("closing %d\n", curr_read_dscrpt);

            close(curr_read_dscrpt);
        }
    }
}

void MIMPI_Finalize()
{
    ASSERT_ZERO(pthread_mutex_lock(&mimpi_handler.mutex));

    mimpi_handler.proc_left_MIMPI[MIMPI_World_rank()] = 1;

    ASSERT_ZERO(pthread_mutex_unlock(&mimpi_handler.mutex));

    int rank = MIMPI_World_rank();

    if(rank == 4)
        printf("proc : %d, sending finalize to proc\n", rank);

    send_finalize_to_all_other_proc();

    if(rank == 4)
        printf("proc : %d, sending finalize to threads\n", rank);

    send_finalize_to_all_threads();
    
    if(rank == 4)
        printf("proc : %d, joining threads\n", rank);
    for (int i = 0; i < mimpi_handler.nbr_of_proc; i++)
    {
        if (i != MIMPI_World_rank())
            ASSERT_ZERO(pthread_join(mimpi_handler.reading_threads[i], NULL));
    }

    if(rank == 4)
        printf("proc : %d, threads joined, closing rest of dscrptrs\n", rank);

    close_all_left_dscrptrs();

    if(rank == 4)
        printf("proc : %d, destructing handler\n", rank);

    handler_destruct(&mimpi_handler);

    if(rank == 4)
        printf("proc : %d, channels finalize\n", rank);

    channels_finalize();
}

//----------MIMPI_SEND----------

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

    // We create buffer in which we store all data we want in specific order so
    // that we can send it via only one send, and that receiver knows our order
    // Firstly we store tag, than count than rest of data.
    unsigned long buffer_size = sizeof(tag) + sizeof(count) + count * sizeof(uint8_t);
    void *buffer = malloc(buffer_size);
    // We memcpy data to our buffer, starting from ptr buffer, but then we need
    // to move our starting pointer by number of saved bytes.
    memcpy(buffer, &tag, sizeof(tag));
    memcpy(buffer + sizeof(tag), &count, sizeof(count));
    memcpy(buffer + sizeof(tag) + sizeof(count), data_to_send,
           count * sizeof(uint8_t));

    // printf("data to send in buffer: \n");
    // for (int i = 0; i < count; i++)
    // {
    //     if(i == 0)
    //         printf("tag:%d ", *((int*)buffer));
    //     else if(i == 1)
    //         printf("count:%d ", *(int*)(buffer + sizeof(tag)));
    //     else
    //         printf("%hhu ", ((uint8_t*)buffer)[i]);
    // }

    // printf("\n");

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

    //  We perform only one send, cause all data is stored in order in buffer.
    int send_ret_code = chsend(MY_STDOUT, buffer, buffer_size);
    // // printf("chsend tag: ret code %d\n", send_ret_code);
    // send_ret_code = chsend(MY_STDOUT, &count, sizeof(count));
    // // printf("chsend count: ret code %d\n", send_ret_code);
    // send_ret_code = chsend(MY_STDOUT, data_to_send, count);

    // printf("chsend data: ret code %d\n", send_ret_code);
    free(buffer);

    if (send_ret_code == -1)
        return MIMPI_ERROR_REMOTE_FINISHED;

    return MIMPI_SUCCESS;
}

//----------MIMPI_RECV----------

void cpy_rec_data_to_dest_set_wanted_flags(void *data, QElem *elem, int count,
                                           int src)
{
    memcpy(data, elem->data, count * sizeof(elem->data[0]));
    QElem_destruct(elem);

    mimpi_handler.wanted_count = COUNT_NOT_WANTED;
    mimpi_handler.wanted_rank = RANK_NOT_WANTED;
    mimpi_handler.wanted_tag = TAG_NOT_WANTED;
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

    MIMPI_Retcode ret_val_of_MIMPI_Recv = MIMPI_SUCCESS;
    bool found_sought_data = false;

    ASSERT_ZERO(pthread_mutex_lock(&mimpi_handler.mutex));
    // printf("Parent searching for elem\n");
    QElem *elem = queue_find_elem(&mimpi_handler.tab_of_queues[source], source, tag, count);

    mimpi_handler.is_sought_data_present = false;
    // printf("Parent ended search: ");
    if (elem != NULL)
    {
        // printf("SUCCES\n");
        found_sought_data = true;
        cpy_rec_data_to_dest_set_wanted_flags(data, elem, count, source);
    }
    else
    {
        // printf("FAILURE\n");
        mimpi_handler.wanted_count = count;
        mimpi_handler.wanted_rank = source;
        mimpi_handler.wanted_tag = tag;
    }

    ASSERT_ZERO(pthread_mutex_unlock(&mimpi_handler.mutex));

    if (found_sought_data)
        return MIMPI_SUCCESS;
    else
    {
        ASSERT_ZERO(pthread_mutex_lock(&mimpi_handler.mutex));

        if (mimpi_handler.proc_left_MIMPI[source] &&
            !mimpi_handler.is_sought_data_present)
        {
            // printf("Parent: src proc ended and also no data present\n");
            ret_val_of_MIMPI_Recv = MIMPI_ERROR_REMOTE_FINISHED;
        }
        else
        {
            // printf("Parent waiting for sb to wake me up\n");
            while (!mimpi_handler.parent_wake_up)
            {
                ASSERT_ZERO(pthread_cond_wait(&mimpi_handler.parent_cond,
                                              &mimpi_handler.mutex));
            }

            // We can be woken up for two reasons: either data we want is on the
            // queue or process we want data from left MIMPI, so our data won't
            // be on our list, thus we search whole list and check if found elem
            // is not NULL
            mimpi_handler.parent_wake_up = false;

            // found_sought_data = true;
            // printf("parent found sought data\n");
            elem = queue_find_elem(&mimpi_handler.tab_of_queues[source], source, tag, count);
            if (elem != NULL)
                cpy_rec_data_to_dest_set_wanted_flags(data, elem, count, source);
            else
                ret_val_of_MIMPI_Recv = MIMPI_ERROR_REMOTE_FINISHED;
        }

        ASSERT_ZERO(pthread_mutex_unlock(&mimpi_handler.mutex));
    }

    return ret_val_of_MIMPI_Recv;
}

void print_Ret_code(MIMPI_Retcode code)
{
    if (code == 0)
        printf("MIMPI_SUCCESS");
    if (code == 3)
        printf("MIMPI_ERROR_REMOTE_FINISHED");
}

void print_tag(int tag)
{
    if (tag == -6)
        printf("MAKE_MIMPI_BARRIER");
    if (tag == -7)
        printf("RELEASE_MIMPI_BARRIER");
    if (tag == -8)
        printf("CANNOT_SYNCH_BARRIER");
    if (tag == -9)
        printf("DEFAULT_TAG");
}

void init_sons_parent_my_rank_idx(int *my_rank, int *parent, int *left_son, int *right_son)
{
    *my_rank = MIMPI_World_rank();
    if (*my_rank % 2 == 0)
        *parent = ((*my_rank) / 2) - 1;
    else
        *parent = ((*my_rank) / 2);
    *left_son = 2 * (*my_rank) + 1;
    *right_son = 2 * (*my_rank) + 2;
}

MIMPI_Retcode informing_helper(int msg, int _tag,
                               int my_parent, int left_son, int right_son)
{
    int message = msg;
    MIMPI_Retcode retcode_send = MIMPI_Send(&message, sizeof(message), my_parent, _tag);

    // If we succeed in sending message to our parent, so
    // ret_val = MIMPI_succes we wait for info with tag RELEASE_MIMPI_BARRIER
    // in main barrier function. If we didnt succeeded in sending message to
    // our parent, this means that our parent is no longer in mimpi thus we need
    // to send message about broken barrier to our subtree. And we can safely
    // leave barrier function with MIMPI_ERROR_REMOTE_FINISHED.
    if (retcode_send != MIMPI_SUCCESS)
    {
        message = CANNOT_SYNCH_BARRIER;
        if (left_son < MIMPI_World_size())
            MIMPI_Send(&message, sizeof(message), left_son, SECOND_STAGE_TAG);
        if (right_son < MIMPI_World_size())
            MIMPI_Send(&message, sizeof(message), right_son, SECOND_STAGE_TAG);

        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode inform_parent_about_state_of_barrier(int message, int tag)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    int ret = informing_helper(message, tag, parent, left_son, right_son);

    // If we get error from inform_func this means that our parent has
    // left MIMPI so we cannot propagete our message higher, so we send
    // CANNOT_SYNCH_BARRIER to sons and end MIMPI_BARRIER with error.
    if (ret == MIMPI_ERROR_REMOTE_FINISHED)
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode Barrier_not_root(MIMPI_Retcode ret_val_recv1,
                               MIMPI_Retcode ret_val_recv2, int *tag1, int *tag2)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    MIMPI_Retcode inform_retcode;

    // If one receive was unsuccessful this means that one of our sons has
    // already ended thus we need to inform our parent that barrier cannot
    // be made.

    if ((ret_val_recv1 != MIMPI_SUCCESS || ret_val_recv2 != MIMPI_SUCCESS) || (*tag1 == CANNOT_SYNCH_BARRIER || *tag2 == CANNOT_SYNCH_BARRIER))
    {
        if(my_rank == 4)
            printf("proc : %d one of my sons ret was not succesful, CANOOT SYNCH, informing parent\n", my_rank);
        
        inform_retcode = inform_parent_about_state_of_barrier(CANNOT_SYNCH_BARRIER, FIRST_STAGE_TAG);

        
        // If we get error from inform_func this means that our parent has
        // left MIMPI so we cannot propagete our message higher.
        // Function handles sending correct messages so we just need to end
        // MIMPI_BARRIER with error.
        if (inform_retcode == MIMPI_ERROR_REMOTE_FINISHED)
        {
            if(my_rank == 4)
                printf("proc : %d ,Parent informed, I got message: ERROR_REMOTE_FINISHED\n", my_rank);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        if(my_rank == 4)
            printf("proc : %d ,Parent informed Successfully \n", my_rank);
            
    }
    else
    {
        if(my_rank == 4)
            printf("proc : %d  receiving from both sons SUCCES, informing parent : %d\n", my_rank, parent);
        inform_retcode = inform_parent_about_state_of_barrier(MAKE_MIMPI_BARRIER, FIRST_STAGE_TAG);
        // printf("proc : %d after successfully informed parent\n", my_rank);
        if (inform_retcode == MIMPI_ERROR_REMOTE_FINISHED)
        {
            if(my_rank == 4)
                printf("proc : %d after success, informed parent, ERROR\n", my_rank);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if(my_rank == 4)
        printf("proc : %d, informing parent SUCCES, waiting for parent response, SECOND STAGE WAITING\n", my_rank);
    // We successfully received messages from our sons, and sending message
    // to our parent was also successful, thus we need to wait for info from
    // him whether release barrier or barrier is broken.
    // printf("proc : %d SECOND STAGE waiting\n", my_rank);
    ret_val_recv1 = MIMPI_Recv(tag1, sizeof(int), parent, SECOND_STAGE_TAG);
    if(my_rank == 4)
    {
        printf("proc : %d SECOND STAGE, after recv message: ", my_rank);
        print_tag(*tag1); printf("\n");
    }
        
    // printf("\n");
    // We got message from our parent, so we forward it to our sons
    int message = *tag1;
    if(my_rank == 4)
        printf("proc : %d, Sending messages to my sons\n", my_rank);
    if (left_son < MIMPI_World_size())
        MIMPI_Send(&message, sizeof(message), left_son, SECOND_STAGE_TAG);
    if (right_son < MIMPI_World_size())
        MIMPI_Send(&message, sizeof(message), right_son, SECOND_STAGE_TAG);

    // Depending on gotten message we return error or success.
    return (message == RELEASE_MIMPI_BARRIER) ? MIMPI_SUCCESS : MIMPI_ERROR_REMOTE_FINISHED;
}

MIMPI_Retcode Barrier_root(MIMPI_Retcode ret_val_recv1,
                           MIMPI_Retcode ret_val_recv2, int *tag1, int *tag2)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    int message;
    // We are root, so if one of our sons returned error we send Cannot synch
    // barrier to our sons
    // printf("root ret val of son1: ");
    // print_Ret_code(ret_val_recv1);
    // printf("\n");
    // printf("root ret val of son2: ");
    // print_Ret_code(ret_val_recv2);
    // printf("\n");
    // printf("tag msg from left son: ");
    // print_tag(*tag1);
    // printf("\n");
    // printf("tag msg from right son: ");
    // print_tag(*tag2);
    // printf("\n");
    if ((ret_val_recv1 != MIMPI_SUCCESS || ret_val_recv2 != MIMPI_SUCCESS) ||
        (*tag1 == CANNOT_SYNCH_BARRIER || *tag2 == CANNOT_SYNCH_BARRIER))
    {
        message = CANNOT_SYNCH_BARRIER;

        if (left_son < MIMPI_World_size())
            MIMPI_Send(&message, sizeof(message), left_son, SECOND_STAGE_TAG);
        if (right_son < MIMPI_World_size())
            MIMPI_Send(&message, sizeof(message), right_son, SECOND_STAGE_TAG);

        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    message = RELEASE_MIMPI_BARRIER;
    // printf("ROOT releasing MIMPI BARRIER\n");
    if (left_son < MIMPI_World_size())
        MIMPI_Send(&message, sizeof(message), left_son, SECOND_STAGE_TAG);
    if (right_son < MIMPI_World_size())
        MIMPI_Send(&message, sizeof(message), right_son, SECOND_STAGE_TAG);

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier()
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    int *tag1 = (int *)malloc(sizeof(int));
    int *tag2 = (int *)malloc(sizeof(int));
    *tag1 = DEFAULT_TAG;
    *tag2 = DEFAULT_TAG;
    MIMPI_Retcode ret_val_recv1 = MIMPI_SUCCESS;
    MIMPI_Retcode ret_val_recv2 = MIMPI_SUCCESS;
    MIMPI_Retcode main_retcode;
    // We wait for info from our left and right son, whether our whole subtree
    // is waiting.

    // printf("BARRIER proc : %d, waiting for msg from sons\n", my_rank);
    if (my_rank == 4)
        printf("proc %d starts bbarrier\n", my_rank);

    if (left_son < MIMPI_World_size())
        ret_val_recv1 = MIMPI_Recv(tag1, sizeof(int), left_son, FIRST_STAGE_TAG);
    if (right_son < MIMPI_World_size())
        ret_val_recv2 = MIMPI_Recv(tag2, sizeof(int), right_son, FIRST_STAGE_TAG);

    if (my_rank == 4)
    {
        printf("proc : %d msg from left son: ", my_rank);
        print_tag(*tag1);
        printf("\n");
        printf("proc : %d msg from right son: ", my_rank);
        print_tag(*tag2);
        printf("\n");
        printf("proc : %d retval 1: ", my_rank);
        print_Ret_code(ret_val_recv1);
        printf("\n");
        printf("proc : %d retval 2: ", my_rank);
        print_Ret_code(ret_val_recv2);
        printf("\n");
    }

    if (my_rank != 0)
    {
        if(my_rank == 4)
            printf("proc : %d going into Barrier_not_root func\n", my_rank);
        main_retcode = Barrier_not_root(ret_val_recv1, ret_val_recv2, tag1, tag2);
        if(my_rank == 4)
            printf("proc : %d RETURNIG from barrier func\n", my_rank);
        free(tag1);
        free(tag2);

        return main_retcode;
    }
    // printf("Root starts doing stuff\n");
    //  We got information that both of our sons
    main_retcode = Barrier_root(ret_val_recv1, ret_val_recv2, tag1, tag2);

    free(tag1);
    free(tag2);

    return main_retcode;
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