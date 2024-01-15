/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
// #include <semaphore.h>
#include <pthread.h>
#include <time.h>
#include <sys/select.h>
#include <stdatomic.h>
#include <errno.h>


typedef struct QElem
{
    int proc_rank;
    int tag;
    int count;
    void *data;
    struct QElem *prev;
    struct QElem *next;
} QElem;

QElem *QElem_make_new(int _rank, int _tag, int _count, void *_data)
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

QElem *queue_remove_elem_from_lst(QueueList *q, QElem *curr_elem)
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
            return queue_remove_elem_from_lst(q, curr_elem);
        }
        curr_elem = curr_elem->prev;
    }

    // We didnt find sought elem so we return NULL.
    return NULL;
}



typedef struct Handler
{
    int nbr_of_proc;
    bool deadlock_enabled;
    // proc_left_MIMPI - array to check when to ret MIMPI_ERROR_REMOTE_FINISHED
    // or if our parent process is in mimpi finalize.
    atomic_bool *proc_left_MIMPI;

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

void handler_init(Handler *handler, bool enable_deadlock)
{
    handler->nbr_of_proc = MIMPI_World_size();
    handler->proc_left_MIMPI = calloc(handler->nbr_of_proc,
                                      sizeof(atomic_bool));
    
    handler->deadlock_enabled = enable_deadlock;

    for (int i = 0; i < handler->nbr_of_proc; i++)
    {
        atomic_store(&handler->proc_left_MIMPI[i], false);
    }

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
    atomic_store(&mimpi_handler.proc_left_MIMPI[proc_rank], true);
    // atomic_store(&mimpi_handler.other_proc_wait_on_receive[proc_rank], 0);

    pthread_mutex_lock(&mimpi_handler.mutex);
    // printf("thread informing that proc source left MIMPI\n");

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

void inform_that_OTHERproc_waits_on_receive(int proc_rank, int message)
{
    pthread_mutex_lock(&mimpi_handler.mutex);

    // If our parent process waits for data from proc of source_rank
    // we need to wake him up, cause he waits for that from SOURCE PROC
    // while SOURCE PROC waits for data from HIM.
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
    // printf("thread %d before pushback when adding new data\n", MIMPI_World_rank());
    queue_push_back(&(mimpi_handler.tab_of_queues[source_rank]), elem);

    // printf("thread %d AFTER pushback when adding new data\n", MIMPI_World_rank());
    //  If elem we added is the one that parent looks for we signal the
    //  parent and give him critical section.
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
    
    //  Needed for fd_set.
    int bigger_stdin =
        (MY_STDIN > MY_STDIN_FROM_PARENT) ? MY_STDIN : MY_STDIN_FROM_PARENT;

    // We receive tag, then count then data.
    int tag;
    int count;
    void *received_data;

    // int ret_code;

    // This set will be used to wait for either tag from src proc or parent proc
    fd_set dscrpt_set_src_and_parent;
    
    while (true)
    {
        // We don't need to acquire mutex, since we only read from array, which
        // is an array of ATOMIC bools.
        // If our parent process invoked MIMPI_finalize we dont want to read
        // data any longer, so we check and break.
        if (atomic_load(&mimpi_handler.proc_left_MIMPI[parent_rank]))
        {
            break;
        }

        // We init our fd_set with two dscrpt that we want to read from.
        // LINUX MAN:
        // Upon return, each of the file descriptor sets is
        // modified in place to indicate which file descriptors are
        // currently "ready".  Thus, if using select() within a loop, the
        // sets must be reinitialized before each call.
        FD_ZERO(&dscrpt_set_src_and_parent);
        FD_SET(MY_STDIN, &dscrpt_set_src_and_parent);
        FD_SET(MY_STDIN_FROM_PARENT, &dscrpt_set_src_and_parent);
        
        select(bigger_stdin + 1, &dscrpt_set_src_and_parent, NULL, NULL, NULL);
        
        // Now we check which dscrpt is still in set, if not MY_STDIN it means
        // that someone wrote sth to MY_STDIN.
        if (FD_ISSET(MY_STDIN, &dscrpt_set_src_and_parent))
        {
            if (atomic_load(&mimpi_handler.proc_left_MIMPI[parent_rank]))
            {
                break;
            }
            // We get message from source proc.
            chrecv(MY_STDIN, &tag, sizeof(tag));
        }
        else
        {
            chrecv(MY_STDIN_FROM_PARENT, &tag, sizeof(tag));
        }

        //  We check the message we got, if its not one of the two below we can
        //  read more data from pipe.
        if (tag == PARENT_PROC_IN_FINALIZE)
        {
            break;
        }
        else if (tag == SRC_PROC_IN_FINALIZE)
        {
            // Message from src proc that it left mIMPI, so we need to inform
            // that it left and if needed wake up my parent process.
            inform_that_SRCproc_left_MIMPI_mutex(source_rank);
            break;
        }
        else if (tag == WAITING_ON_REC_TAG)
        {
            // I'm process with higher rank than the one sending msg
            chrecv(MY_STDIN, &count, sizeof(count));
            received_data = malloc(count);

            chrecv(MY_STDIN, received_data, PIPE_READ_SIZE);

            QElem *elem = QElem_make_new(source_rank, tag, count, received_data);
            
            add_received_data_to_MIMPI_mutex(elem, source_rank);
            inform_that_OTHERproc_waits_on_receive(source_rank, WAITING_ON_REC_TAG);
        }
        else if (tag == NO_LONGER_WAITING_ON_REC_TAG)
        {
            chrecv(MY_STDIN, &count, sizeof(count));
            received_data = malloc(count);

            chrecv(MY_STDIN, received_data, PIPE_READ_SIZE);

            QElem *elem = QElem_make_new(source_rank, tag, count, received_data);

            add_received_data_to_MIMPI_mutex(elem, source_rank);

            inform_that_OTHERproc_waits_on_receive(source_rank, NO_LONGER_WAITING_ON_REC_TAG);
        }
        else if (tag == FOUND_DEADLOCK_TAG)
        {
            chrecv(MY_STDIN, &count, sizeof(count));
            received_data = malloc(count);

            chrecv(MY_STDIN, received_data, PIPE_READ_SIZE);

            QElem *elem = QElem_make_new(source_rank, tag, count, received_data);

            add_received_data_to_MIMPI_mutex(elem, source_rank);

            inform_that_OTHERproc_waits_on_receive(source_rank, FOUND_DEADLOCK_TAG);
        }
        else
        {

            // After receiving count = how many bytes we will read in chrecv
            // we need to allocate that many bytes in our received_data variable
            // so that we could store all read data and in future parent process
            // could copy this data.
            chrecv(MY_STDIN, &count, sizeof(count));

            received_data = malloc(count);
            int read_bytes = 0;
            int bytes_left = 0;
            
            // Count might be greater than pipes buffor so we need to read from
            // buffor till read_bytes are equal to our count.
            while (read_bytes < count)
            {
                bytes_left = count - read_bytes; 
                if (bytes_left > PIPE_READ_SIZE)
                {
                    read_bytes += chrecv(MY_STDIN, received_data + read_bytes,
                                         PIPE_READ_SIZE);
                }
                else
                {
                    read_bytes += chrecv(MY_STDIN, received_data + read_bytes,
                                         bytes_left);
                }

            }
            
            QElem *elem = QElem_make_new(source_rank, tag, count, received_data);

            // add_received_data acquires mutex, adds data to queue and if added
            // data is a wanted data it wakes up parent proc.
            add_received_data_to_MIMPI_mutex(elem, source_rank);
        }
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
        curr_rank = 0;

        if (curr_proc == my_rank)
        {
            //   When curr_proc is equal to our rank, we close only reading ends
            //   of our pipes, since we will use them to write, and others will
            //   read from them.
            while (curr_rank < nbr_proc)
            {
                if (curr_proc != curr_rank)
                {
                    close(curr_read_dscrpt);
                }
                curr_rank++;
                curr_read_dscrpt += 2;
            }
        }
        else // curr_proc != my_rank
        {
            while (curr_rank < nbr_proc)
            {
                // If curr_proc is not us, we close all pipes except our pipe
                // in curr_proc. In our pipe we close read end of pipe.
                if (my_rank != curr_rank)
                {
                    close(curr_read_dscrpt);
                    close(curr_read_dscrpt + 1);
                }
                else
                {
                    close(curr_read_dscrpt + 1);
                }
                curr_rank++;
                curr_read_dscrpt += 2;
            }
        }

        curr_proc++;
    }
}

void MIMPI_Init(bool enable_deadlock_detection)
{
    channels_init();
    close_redundant_dscrpt();
    handler_init(&mimpi_handler, enable_deadlock_detection);

    int my_rank = MIMPI_World_rank();

    for (int proc_rank = 0; proc_rank < mimpi_handler.nbr_of_proc; proc_rank++)
    {
        if (proc_rank != my_rank)
        {
            // POSSIBLE MEMORY LEAK!
            int *rank_for_thread = (int *)malloc(sizeof(int));
            *rank_for_thread = proc_rank;

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
    // Read dscrpt are first ones, write are second ones, so MY_STARTING_DSCRPT
    // is a first read dscrpt, thus each time we find read dscrpt and to get
    // write dscrpt we need to add 1.
    // !!!!! POSSIBLE THAT IN MIMPI PROC_LEFT_MIMPI
    int tag = PARENT_PROC_IN_FINALIZE;
    for (int i = 0; i < nbr_proc; i++)
    {
        if (i != my_rank)
        {
            if (!atomic_load(&mimpi_handler.proc_left_MIMPI[i]))
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
            if (!atomic_load(&mimpi_handler.proc_left_MIMPI[proc_rank]))
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
    
    for (int curr_proc = 0; curr_proc < nbr_proc; curr_proc++)
    {
        curr_read_dscrpt = OFFSET + curr_proc * 2 * nbr_proc;

        if (curr_proc == my_rank)
        {
            i = 0;
            
            while (i < nbr_proc)
            {
                // In my_rank process we left opened all write ends of pipes,
                // so we need to close them now.
                if (i != my_rank)
                {
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

            close(curr_read_dscrpt);
        }
    }
}

void MIMPI_Finalize()
{
    ASSERT_ZERO(pthread_mutex_lock(&mimpi_handler.mutex));

    atomic_store(&mimpi_handler.proc_left_MIMPI[MIMPI_World_rank()], true);

    ASSERT_ZERO(pthread_mutex_unlock(&mimpi_handler.mutex));

    send_finalize_to_all_other_proc();
    send_finalize_to_all_threads();

    for (int i = 0; i < mimpi_handler.nbr_of_proc; i++)
    {
        if (i != MIMPI_World_rank())
        {
            ASSERT_ZERO(pthread_join(mimpi_handler.reading_threads[i], NULL));
        }
    }

    close_all_left_dscrptrs();
    handler_destruct(&mimpi_handler);
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

    if (atomic_load(&mimpi_handler.proc_left_MIMPI[destination]))
        return MIMPI_ERROR_REMOTE_FINISHED;
    // We want array of bytes, so we need to cast void ptr to unint8_t.

    // We create buffer in which we store all data we want in specific order so
    // that we can send it via only one send, and that receiver knows our order
    // Firstly we store tag, than count than rest of data.
    unsigned long buffer_size = sizeof(tag) + sizeof(count) + count;
    void *buffer = malloc(buffer_size);

    // We memcpy data to our buffer, starting from ptr buffer, but then we need
    // to move our starting pointer by number of saved bytes.
    memcpy(buffer, &tag, sizeof(tag));
    memcpy(buffer + sizeof(tag), &count, sizeof(count));
    memcpy(buffer + sizeof(tag) + sizeof(count), data, count);

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
    int sended_bytes = 0;
    while(sended_bytes < buffer_size)
    {
        sended_bytes += chsend(MY_STDOUT, buffer + sended_bytes, buffer_size - sended_bytes);
    }   
        

    free(buffer);

    if (sended_bytes == -1)
        return MIMPI_ERROR_REMOTE_FINISHED;

    return MIMPI_SUCCESS;
}

//----------MIMPI_RECV----------
void set_wanted_flags_to_NOT_WANTED()
{
    mimpi_handler.wanted_count = COUNT_NOT_WANTED;
    mimpi_handler.wanted_rank = RANK_NOT_WANTED;
    mimpi_handler.wanted_tag = TAG_NOT_WANTED;
}

void cpy_rec_data_to_dest_set_wanted_flags(void *data, QElem *elem, int count,
                                           int src)
{
    memcpy(data, elem->data, count * sizeof(elem->data[0]));
    QElem_destruct(elem);

    set_wanted_flags_to_NOT_WANTED();
}

/**
 * Function sends message to source about possible deadlock, information about
 * deadlock is in TAG parameter
 */
void inform_SRCproc_about_possible_deadlock(int source, int deadlock_tag)
{
    uint8_t some_data = 1;
    MIMPI_Send(&some_data, sizeof(some_data), source, deadlock_tag);
}

bool older_proc_deadlock_detection(int source, QElem *elem,
                                   MIMPI_Retcode *ret_val)
{
    bool deadlock = false;

    if (mimpi_handler.deadlock_enabled &&
        MIMPI_World_rank() > source)
    {
        //printf("OLDproc %d looking for deadlock msgs\n", MIMPI_World_rank());
        //  If we are older proc, we check if we got any msg from source
        //  about it waiting to receive from us.
        elem = queue_find_elem(&mimpi_handler.tab_of_queues[source], source, WAITING_ON_REC_TAG, sizeof(uint8_t));
        
        if (elem != NULL)
        {
            QElem_destruct(elem);
            // If we found such message we check again whether source
            // sent another msg that it is no longer waiting, cause
            // while it was waiting it might got data it was waiting for
            // SIZEOF data we want to find is uint8_t since in inform we send
            // some data which is one uint8.
            elem = queue_find_elem(&mimpi_handler.tab_of_queues[source], source, NO_LONGER_WAITING_ON_REC_TAG, sizeof(uint8_t));

            if (elem == NULL)
            {
                //  If another msg is not present we have DEADLOCK.
                deadlock = true;
                *ret_val = MIMPI_ERROR_DEADLOCK_DETECTED;
                set_wanted_flags_to_NOT_WANTED();
                inform_SRCproc_about_possible_deadlock(source, FOUND_DEADLOCK_TAG);
            }
            else
            {
                QElem_destruct(elem);
            }
        }
    }

    return deadlock;
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

    MIMPI_Retcode ret_val_of_Recv = MIMPI_SUCCESS;

    bool found_sought_data = false;
   
    ASSERT_ZERO(pthread_mutex_lock(&mimpi_handler.mutex));

    QElem *elem = queue_find_elem(&mimpi_handler.tab_of_queues[source], source, tag, count);
    mimpi_handler.is_sought_data_present = false;

    if (elem != NULL)
    {
        found_sought_data = true;
        cpy_rec_data_to_dest_set_wanted_flags(data, elem, count, source);
    }
    else
    {
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

        //  In the meantime data we want could be sent, so we check.
        elem = queue_find_elem(&mimpi_handler.tab_of_queues[source], source, tag, count);

        if (elem != NULL)
        {
            mimpi_handler.parent_wake_up = false;
            ret_val_of_Recv = MIMPI_SUCCESS;

            cpy_rec_data_to_dest_set_wanted_flags(data, elem, count, source);
        }
        else
        {
            // We know for sure now that data we want is not present.
            bool deadlock = older_proc_deadlock_detection(source, elem,
                                                          &ret_val_of_Recv);

            if (!deadlock)
            {
                if (atomic_load(&mimpi_handler.proc_left_MIMPI[source]) &&
                    !mimpi_handler.is_sought_data_present)
                {
                    //  We didnt find data and src proc left mimpi, so we end.
                    ret_val_of_Recv = MIMPI_ERROR_REMOTE_FINISHED;
                }
                else
                {
                    //  If we are younger proc, we didnt find wanted data, so
                    //  just before we start waiting we tell this to older proc.
                    if (MIMPI_World_rank() < source &&
                        mimpi_handler.deadlock_enabled &&
                        !mimpi_handler.parent_wake_up)
                    {
                        inform_SRCproc_about_possible_deadlock(source, WAITING_ON_REC_TAG);
                    }
                    
                    while (!mimpi_handler.parent_wake_up)
                    {
                        ASSERT_ZERO(pthread_cond_wait(&mimpi_handler.parent_cond, &mimpi_handler.mutex));
                    }
                    
                    //  We are woken up, there are three reasons:
                    //  1) Data we want is present
                    //  2) Src proc left mimpi
                    //  3) There is a deadlock
                    set_wanted_flags_to_NOT_WANTED();
                    mimpi_handler.parent_wake_up = false;

                    // 1) We check if wanted data present/
                    elem = queue_find_elem(&mimpi_handler.tab_of_queues[source], source, tag, count);

                    if (elem != NULL)
                    {
                        //  1) Wanted data present.
                        ret_val_of_Recv = MIMPI_SUCCESS;
                        cpy_rec_data_to_dest_set_wanted_flags(data, elem, count, source);
                        // 1) If we're younger proc we inform about succes older
                        if (mimpi_handler.deadlock_enabled &&
                            MIMPI_World_rank() < source)
                        {
                            inform_SRCproc_about_possible_deadlock(source, NO_LONGER_WAITING_ON_REC_TAG);
                        }
                    }
                    else
                    {
                        //  2) or 3) - we didnt find wanted data, even though
                        //  we were woken up. So either deadlock or src lft mimpi
                        bool is_deadlock = false;

                        if (mimpi_handler.deadlock_enabled &&
                            MIMPI_World_rank() < source)
                        {
                            //  3) We are younger proc so we check if we get msg
                            //  from older about deadlock.
                            elem = queue_find_elem(&mimpi_handler.tab_of_queues[source], source, FOUND_DEADLOCK_TAG,
                                                   sizeof(uint8_t));

                            if (elem != NULL)
                            {
                                //  3) There is a deadlock, we return error.
                                QElem_destruct(elem);
                                ret_val_of_Recv = MIMPI_ERROR_DEADLOCK_DETECTED;
                                is_deadlock = true;
                            }
                        }
                        if (mimpi_handler.deadlock_enabled &&
                            MIMPI_World_rank() > source)
                        {
                            //  3) We are older proc, we check msgs from younger
                            //  proc, if there is only one this means 3) deadlock
                            //  if there are two, this means 2) src left mimpi
                            is_deadlock = older_proc_deadlock_detection(source, elem, &ret_val_of_Recv);
                        }

                        if (!is_deadlock)
                        {
                            // If we were woken up and there is no deadlock it
                            // means that src proc left mimpi.
                            ret_val_of_Recv = MIMPI_ERROR_REMOTE_FINISHED;
                        }
                    }
                }
            }
        }
        ASSERT_ZERO(pthread_mutex_unlock(&mimpi_handler.mutex));
    }
    return ret_val_of_Recv;
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

MIMPI_Retcode informing_helper(uint8_t msg, int _tag,
                               int my_parent, int left_son, int right_son)
{
    uint8_t message = msg;
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

MIMPI_Retcode inform_parent_about_state_of_barrier(uint8_t message, int tag)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    MIMPI_Retcode ret = informing_helper(message, tag, parent, left_son, right_son);

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
                               MIMPI_Retcode ret_val_recv2, uint8_t *tag1,
                               uint8_t *tag2)
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
        // if(my_rank == 4)
        // printf("proc : %d one of my sons ret was not succesful, CANOOT SYNCH, informing parent\n", my_rank);

        inform_retcode = inform_parent_about_state_of_barrier(CANNOT_SYNCH_BARRIER, FIRST_STAGE_TAG);

        // If we get error from inform_func this means that our parent has
        // left MIMPI so we cannot propagete our message higher.
        // Function handles sending correct messages so we just need to end
        // MIMPI_BARRIER with error.
        if (inform_retcode == MIMPI_ERROR_REMOTE_FINISHED)
        {
            // if(my_rank == 4)
            // printf("proc : %d ,Parent informed, I got message: ERROR_REMOTE_FINISHED\n", my_rank);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // if(my_rank == 4)
        // printf("proc : %d ,Parent informed Successfully \n", my_rank);
    }
    else
    {
        // if(my_rank == 4)
        // printf("proc : %d  receiving from both sons SUCCES, informing parent : %d\n", my_rank, parent);
        inform_retcode = inform_parent_about_state_of_barrier(MAKE_MIMPI_BARRIER, FIRST_STAGE_TAG);
        // printf("proc : %d after successfully informed parent\n", my_rank);
        if (inform_retcode == MIMPI_ERROR_REMOTE_FINISHED)
        {
            // if(my_rank == 4)
            // printf("proc : %d after success, informed parent, ERROR\n", my_rank);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    // if(my_rank == 4)
    // printf("proc : %d, informing parent SUCCES, waiting for parent response, SECOND STAGE WAITING\n", my_rank);
    // We successfully received messages from our sons, and sending message
    // to our parent was also successful, thus we need to wait for info from
    // him whether release barrier or barrier is broken.
    // printf("proc : %d SECOND STAGE waiting\n", my_rank);
    uint8_t message;
    ret_val_recv1 = MIMPI_Recv(&message, sizeof(message), parent, SECOND_STAGE_TAG);
    // if(my_rank == 4)
    // {
    // printf("proc : %d SECOND STAGE, after recv message: ", my_rank);
    // print_msg(*tag1); printf("\n");
    // }

    // printf("\n");
    // We got message from our parent, so we forward it to our sons
    // int message = *tag1;
    // if(my_rank == 4)
    // printf("proc : %d, Sending messages to my sons\n", my_rank);
    if (left_son < MIMPI_World_size())
        MIMPI_Send(&message, sizeof(message), left_son, SECOND_STAGE_TAG);
    if (right_son < MIMPI_World_size())
        MIMPI_Send(&message, sizeof(message), right_son, SECOND_STAGE_TAG);

    // Depending on gotten message we return error or success.
    return (message == RELEASE_MIMPI_BARRIER) ? MIMPI_SUCCESS : MIMPI_ERROR_REMOTE_FINISHED;
}

MIMPI_Retcode Barrier_root(MIMPI_Retcode ret_val_recv1,
                           MIMPI_Retcode ret_val_recv2, uint8_t *tag1,
                           uint8_t *tag2)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    uint8_t message;
    // We are root, so if one of our sons returned error we send Cannot synch
    // barrier to our sons
    // printf("root ret val of son1: ");
    // print_Ret_code(ret_val_recv1);
    // printf("\n");
    // printf("root ret val of son2: ");
    // print_Ret_code(ret_val_recv2);
    // printf("\n");
    // printf("tag msg from left son: ");
    // print_msg(*tag1);
    // printf("\n");
    // printf("tag msg from right son: ");
    // print_msg(*tag2);
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
    // printf("ROOT sent msg to left son\n");
    if (right_son < MIMPI_World_size())
        MIMPI_Send(&message, sizeof(message), right_son, SECOND_STAGE_TAG);
    // printf("ROOT sent msg to right son\n");
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier()
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    // Allocating memory for tags is stupid but dont want to change it since it
    // works just fine.
    uint8_t *tag1 = (uint8_t *)malloc(sizeof(uint8_t));
    uint8_t *tag2 = (uint8_t *)malloc(sizeof(uint8_t));
    *tag1 = DEFAULT_TAG;
    *tag2 = DEFAULT_TAG;
    MIMPI_Retcode ret_val_recv1 = MIMPI_SUCCESS;
    MIMPI_Retcode ret_val_recv2 = MIMPI_SUCCESS;
    MIMPI_Retcode main_retcode;
    // We wait for info from our left and right son, whether our whole subtree
    // is waiting.

    // printf("BARRIER proc : %d, waiting for msg from sons\n", my_rank);
    // if (my_rank == 4)
    // printf("proc %d starts bbarrier\n", my_rank);

    if (left_son < MIMPI_World_size())
        ret_val_recv1 = MIMPI_Recv(tag1, sizeof(*tag1), left_son, FIRST_STAGE_TAG);
    // printf("proc : %d msg from left son: ", my_rank);
    // print_msg(*tag1);
    // printf("\n");

    if (right_son < MIMPI_World_size())
        ret_val_recv2 = MIMPI_Recv(tag2, sizeof(*tag2), right_son, FIRST_STAGE_TAG);

    // if (my_rank == 4)
    // {

    // printf("proc : %d msg from right son: ", my_rank);
    // print_msg(*tag2);
    // printf("\n");
    // printf("proc : %d retval 1: ", my_rank);
    // print_Ret_code(ret_val_recv1);
    // printf("\n");
    // printf("proc : %d retval 2: ", my_rank);
    // print_Ret_code(ret_val_recv2);
    // printf("\n");
    // }

    if (my_rank != 0)
    {
        // if(my_rank == 4)
        // printf("proc : %d going into Barrier_not_root func\n", my_rank);
        main_retcode = Barrier_not_root(ret_val_recv1, ret_val_recv2, tag1, tag2);
        // if(my_rank == 4)
        // printf("proc : %d RETURNIG from barrier func\n", my_rank);
        free(tag1);
        free(tag2);

        return main_retcode;
    }
    // printf("Root starts doing stuff\n");
    //   We got information that both of our sons
    main_retcode = Barrier_root(ret_val_recv1, ret_val_recv2, tag1, tag2);

    free(tag1);
    free(tag2);

    return main_retcode;
}

MIMPI_Retcode root_BCAST(void *data,
                         int count,
                         int root,
                         MIMPI_Retcode ret_code)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);
    MIMPI_Retcode l_son_retcode, r_son_retcode;
    l_son_retcode = MIMPI_SUCCESS;
    r_son_retcode = MIMPI_SUCCESS;

    uint8_t message;
    // We check if we as real root got data, if not we send cannot bcast.
    if (ret_code == MIMPI_SUCCESS)
        message = MAKE_BCAST;
    else
        message = CANNOT_BCAST;
    
    void *data_for_sons = malloc(count + 1);
    *(uint8_t *)data_for_sons = message;

    memcpy(data_for_sons + 1, data, count);

    if (left_son < MIMPI_World_size())
        l_son_retcode = MIMPI_Send(data_for_sons, count + 1, left_son, FIRST_STAGE_TAG);

    if (right_son < MIMPI_World_size())
        r_son_retcode = MIMPI_Send(data_for_sons, count + 1, right_son, FIRST_STAGE_TAG);

    free(data_for_sons);

    if (message == CANNOT_BCAST)
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    // Now we wait for sons response
    uint8_t l_son_message = MAKE_BCAST;
    uint8_t r_son_message = MAKE_BCAST;

    if (left_son < MIMPI_World_size() && l_son_retcode == MIMPI_SUCCESS)
        l_son_retcode = MIMPI_Recv(&l_son_message,
                                   sizeof(l_son_message), left_son, FIRST_STAGE_TAG);

    if (right_son < MIMPI_World_size() && r_son_retcode == MIMPI_SUCCESS)
        r_son_retcode = MIMPI_Recv(&r_son_message,
                                   sizeof(r_son_message), right_son, FIRST_STAGE_TAG);

    // If at lest one sends wasn't succesful we propagate error.
    // We send error only to the son that didnt return error, cause the
    // other son knows that it needs to propagate the error.
    if ((r_son_retcode != MIMPI_SUCCESS ||
         l_son_retcode != MIMPI_SUCCESS) ||
        (l_son_message == CANNOT_BCAST ||
         r_son_message == CANNOT_BCAST))
    {
        message = CANNOT_BCAST;
        if (left_son < MIMPI_World_size() && l_son_retcode == MIMPI_SUCCESS &&
            l_son_message != CANNOT_BCAST)
            MIMPI_Send(&message, sizeof(message), left_son, FIRST_STAGE_TAG);
        if (right_son < MIMPI_World_size() && r_son_retcode == MIMPI_SUCCESS &&
            r_son_message != CANNOT_BCAST)
            MIMPI_Send(&message, sizeof(message), right_son, FIRST_STAGE_TAG);

        // After sending messages we end with error.
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else
    {
        message = MAKE_BCAST;

        if (left_son < MIMPI_World_size())
            MIMPI_Send(&message, sizeof(message), left_son, FIRST_STAGE_TAG);
        
        if (right_son < MIMPI_World_size())
            MIMPI_Send(&message, sizeof(message), right_son, FIRST_STAGE_TAG);
    }
    return MIMPI_SUCCESS;
}

void cannot_Bcast_handler_not_root(MIMPI_Retcode parent_retcode,
                                   MIMPI_Retcode l_son_retcode, MIMPI_Retcode r_son_retcode,
                                   uint8_t l_son_message, uint8_t r_son_message)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;

    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    uint8_t message = CANNOT_BCAST;

    if (left_son < MIMPI_World_size() &&
        l_son_message != CANNOT_BCAST &&
        l_son_retcode == MIMPI_SUCCESS)
    {
        MIMPI_Send(&message, sizeof(message), left_son, FIRST_STAGE_TAG);
    }
    if (right_son < MIMPI_World_size() &&
        r_son_message != CANNOT_BCAST &&
        r_son_retcode == MIMPI_SUCCESS)
    {
        MIMPI_Send(&message, sizeof(message), right_son, FIRST_STAGE_TAG);
    }

    if (parent_retcode == MIMPI_SUCCESS)
        MIMPI_Send(&message, sizeof(message), parent, FIRST_STAGE_TAG);
}

MIMPI_Retcode not_root_BCAST(void *data,
                             int count,
                             int root)
{
    // We are just a process who waits for data from parent and propagets it
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    MIMPI_Retcode l_son_retcode, r_son_retcode;
    l_son_retcode = MIMPI_SUCCESS;
    r_son_retcode = MIMPI_SUCCESS;
    uint8_t message_from_parent = CANNOT_BCAST;

    void *data_from_parent = malloc(count + 1);

    // printf("proc: %d in not_root_BCAST, receiving msg from parent: %d\n", my_rank, parent);

    MIMPI_Retcode parent_ret_code = MIMPI_Recv(data_from_parent,
                                               count + 1, parent, FIRST_STAGE_TAG);
    if (parent_ret_code != MIMPI_SUCCESS)
    {
        memcpy(data_from_parent, &message_from_parent, 1);
        memcpy(data_from_parent + 1, data, count);
    }
    else
    {
        message_from_parent = *(uint8_t *)data_from_parent;
        memcpy(data, data_from_parent + 1, count);
    }

    // printf("proc %d received MSG from parent %d: ", my_rank, parent); print_msg(message_from_parent);
    if (left_son < MIMPI_World_size())
        l_son_retcode = MIMPI_Send(data_from_parent, count + 1, left_son, FIRST_STAGE_TAG);

    if (right_son < MIMPI_World_size())
        r_son_retcode = MIMPI_Send(data_from_parent, count + 1, right_son, FIRST_STAGE_TAG);

    free(data_from_parent);

    if (message_from_parent == CANNOT_BCAST)
    {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    uint8_t l_son_message = MAKE_BCAST;
    uint8_t r_son_message = MAKE_BCAST;
    // printf("proc %d waiting for sons msgs\n", my_rank);
    if (left_son < MIMPI_World_size())
        l_son_retcode = MIMPI_Recv(&l_son_message,
                                   sizeof(l_son_message), left_son, FIRST_STAGE_TAG);

    if (right_son < MIMPI_World_size())
        r_son_retcode = MIMPI_Recv(&r_son_message,
                                   sizeof(r_son_message), right_son, FIRST_STAGE_TAG);

    if ((r_son_retcode != MIMPI_SUCCESS ||
         l_son_retcode != MIMPI_SUCCESS) ||
        (l_son_message == CANNOT_BCAST ||
         r_son_message == CANNOT_BCAST))
    {
        // printf("proc %d got some error from sons\n", my_rank);
        cannot_Bcast_handler_not_root(parent_ret_code, l_son_retcode, r_son_retcode, l_son_message, r_son_message);

        // After sending messages we end with error.
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else
    {
        // Everything was succesful so we send SUCCESSFUL_BCAST to our
        // parent and wait for data from him
        // printf("proc %d SUCCES, we send to our parent %d : MAKE_BCAST\n", my_rank, parent);
        uint8_t message = MAKE_BCAST;
        MIMPI_Send(&message, sizeof(message), parent, FIRST_STAGE_TAG);
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root)
{
    if (root >= MIMPI_World_size() || root < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;

    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    // Root is us
    if (root == MIMPI_World_rank())
    {
        // printf("proc %d is root\n", my_rank);
        //  If we are real root of the tree, meaning == 0, we send info that we
        //  want to Bcast, wait for sons response and if we can we send DATA,
        //  if we cant we send error to our sons.
        if (root == 0)
        {
            return root_BCAST(data, count, root, MIMPI_SUCCESS);
        }
        // If we are not real root, we send data to real root and do what other
        // normal processes do.
        else
        {
            // printf("rootproc %d, sends data to real root\n", my_rank);
            MIMPI_Send(data, count, 0, FIRST_STAGE_TAG);
        }
    }
    else if (MIMPI_World_rank() == 0)
    {
        // We are real root, but not root given by function, so we need to wait
        // for data from given root.
        MIMPI_Retcode ret_code = MIMPI_Recv(data, count, root, FIRST_STAGE_TAG);
        // printf("REAL ROOT received data %d\n", *(uint8_t*)data);
        return root_BCAST(data, count, root, ret_code);
    }

    return not_root_BCAST(data, count, root);
}

void perform_given_operation(void *res_data,
                             void *son_data, int count, MIMPI_Op op)
{
    // We need to cast our data to easily operate on it.
    uint8_t *son = (uint8_t *)son_data;
    uint8_t *res = (uint8_t *)res_data;

    for (int i = 0; i < count; i++)
    {
        if (op == MIMPI_MAX)
        {
            if (res[i] < son[i])
                res[i] = son[i];
        }
        else if (op == MIMPI_MIN)
        {
            if (res[i] > son[i])
                res[i] = son[i];
        }
        else if (op == MIMPI_SUM)
        {
            res[i] += son[i];
        }
        else if (op == MIMPI_PROD)
        {
            res[i] *= son[i];
        }
        else
        {
            printf("NO SUCH OPERATION EXISTS\n");
        }
    }
}

void receive_data_from_sons(int left_son, int right_son, void *l_son_data,
                            void *r_son_data, int count, MIMPI_Retcode *retcode_l_son,
                            MIMPI_Retcode *retcode_r_son)
{
    if (left_son < MIMPI_World_size())
        *retcode_l_son = MIMPI_Recv(l_son_data, count, left_son, FIRST_STAGE_TAG);

    if (right_son < MIMPI_World_size())
        *retcode_r_son = MIMPI_Recv(r_son_data, count, right_son, FIRST_STAGE_TAG);
}

void send_msgs_to_sons(uint8_t message, int left_son, int right_son,
                       uint8_t l_son_info, uint8_t r_son_info)
{
    if (left_son < MIMPI_World_size() && l_son_info != NO_MSG_REDUCE)
        MIMPI_Send(&message, sizeof(message), left_son, FIRST_STAGE_TAG);

    if (right_son < MIMPI_World_size() && r_son_info != NO_MSG_REDUCE)
        MIMPI_Send(&message, sizeof(message), right_son, FIRST_STAGE_TAG);
}

void print_data(void *data, int count)
{
    for (int i = 0; i < count; i++)
    {
        printf("%i ", (*(uint8_t *)(data + i)));
    }
    printf("\n");
}

MIMPI_Retcode real_root_REDUCE(void const *send_data,
                               void *recv_data,
                               int count,
                               MIMPI_Op op,
                               int root)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    MIMPI_Retcode whole_func_retcode = MIMPI_SUCCESS;
    MIMPI_Retcode retcode_l_son = MIMPI_SUCCESS;
    MIMPI_Retcode retcode_r_son = MIMPI_SUCCESS;

    void *l_son_data = malloc(count + 1);
    void *r_son_data = malloc(count + 1);
    // We need to initialize data[0] with sth to easily check if we got msg.
    *(uint8_t *)l_son_data = NO_MSG_REDUCE;
    *(uint8_t *)r_son_data = NO_MSG_REDUCE;

    // We are root, we wait for data from our sons to perform operation on
    // received data[0] has message from our sons to us
    receive_data_from_sons(left_son, right_son, l_son_data, r_son_data,
                           count + 1, &retcode_l_son, &retcode_r_son);
    // printf("ROOT left_son%d data: \n", left_son);

    // print_data(l_son_data + 1, count);

    // printf("ROOT right_son%d data: \n", right_son);

    // print_data(r_son_data + 1, count);

    if (retcode_l_son == MIMPI_SUCCESS &&
        retcode_r_son == MIMPI_SUCCESS &&
        *(uint8_t *)l_son_data != CANNOT_REDUCE &&
        *(uint8_t *)r_son_data != CANNOT_REDUCE)
    {
        // We need temp void* to store results of operations since,
        // send data is CONST.
        void *res_data = malloc(count);
        memcpy(res_data, send_data, count);

        if (left_son < MIMPI_World_size())
            perform_given_operation(res_data, l_son_data + 1, count, op);
        if (right_son < MIMPI_World_size())
            perform_given_operation(res_data, r_son_data + 1, count, op);

        // printf("ROOT RESULT DATA: ");
        // print_data(res_data, count);
        // If we are given root (root = 0) we simply copy data
        // to recv data and signalize sons about success
        if (root == MIMPI_World_rank())
        {
            memcpy(recv_data, res_data, count);
        }
        // If 0 is not a root in argument we send data to given root
        // without any tags, cause info whether REDUCE successful root
        // will get from his parent.
        else
        {
            MIMPI_Send(res_data, count, root, FIRST_STAGE_TAG);
        }

        free(res_data);
        // Sending msgs to sons that REDUCE successful.
        send_msgs_to_sons(MAKE_REDUCE, left_son, right_son,
                          *(uint8_t *)l_son_data, *(uint8_t *)r_son_data);

        whole_func_retcode = MIMPI_SUCCESS;
    }
    else
    {
        // Either on of our sons left MIMPI or in one of subtrs sb left.
        // Firstly we need to send data to given root, data needs to be of
        // size count, doesnt matter what we send, but we must do it since
        // root process will wait for this data.
        MIMPI_Send(send_data, count, root, FIRST_STAGE_TAG);

        // We send msg about error to our sons but only if their retcodes
        // were MIMPI_SUCCESS otherwise it means they left MIMPI so we dont
        // need to send them any message.
        send_msgs_to_sons(CANNOT_REDUCE, left_son, right_son,
                          *(uint8_t *)l_son_data, *(uint8_t *)r_son_data);

        whole_func_retcode = MIMPI_ERROR_REMOTE_FINISHED;
    }

    free(l_son_data);
    free(r_son_data);

    return whole_func_retcode;
}

MIMPI_Retcode inform_parent_about_REDUCE(uint8_t message, const void *send_data,
                                         int count, int parent)
{
    void *data_for_parent = malloc(count + 1);
    *(uint8_t *)data_for_parent = message;

    memcpy(data_for_parent + 1, send_data, count);

    MIMPI_Retcode ret = MIMPI_Send(data_for_parent, count + 1, parent, FIRST_STAGE_TAG);

    free(data_for_parent);

    return ret;
}

MIMPI_Retcode other_proc_REDUCE(void const *send_data,
                                void *recv_data,
                                int count,
                                MIMPI_Op op,
                                int root)
{
    int my_rank;
    int parent;
    int left_son;
    int right_son;
    init_sons_parent_my_rank_idx(&my_rank, &parent, &left_son, &right_son);

    MIMPI_Retcode whole_func_retcode = MIMPI_SUCCESS;
    MIMPI_Retcode retcode_l_son = MIMPI_SUCCESS;
    MIMPI_Retcode retcode_r_son = MIMPI_SUCCESS;

    void *l_son_data = malloc(count + 1);
    void *r_son_data = malloc(count + 1);
    // We need to initialize data[0] with sth to easily check if we got msg.
    *(uint8_t *)l_son_data = NO_MSG_REDUCE;
    *(uint8_t *)r_son_data = NO_MSG_REDUCE;
    // printf("proc %d starts REDUCE : receiving data from sons\n", my_rank);
    receive_data_from_sons(left_son, right_son, l_son_data, r_son_data,
                           count + 1, &retcode_l_son, &retcode_r_son);

    // If receiving from sons was successful we perform operations and send
    // new data to our parent with information tag.
    if (retcode_l_son == MIMPI_SUCCESS &&
        retcode_r_son == MIMPI_SUCCESS &&
        *(uint8_t *)l_son_data != CANNOT_REDUCE &&
        *(uint8_t *)r_son_data != CANNOT_REDUCE)
    {
        void *res_data = malloc(count);
        memcpy(res_data, send_data, count);
        if (left_son < MIMPI_World_size())
            perform_given_operation(res_data, l_son_data + 1, count, op);
        if (right_son < MIMPI_World_size())
            perform_given_operation(res_data, r_son_data + 1, count, op);

        MIMPI_Retcode ret = inform_parent_about_REDUCE(MAKE_REDUCE, res_data, count, parent);

        free(res_data);
        // Its possible that our parent is not in MIMPI so we need to send
        // error message to our sons
        if (ret != MIMPI_SUCCESS)
        {
            send_msgs_to_sons(CANNOT_REDUCE, left_son, right_son,
                              *(uint8_t *)l_son_data, *(uint8_t *)r_son_data);

            whole_func_retcode = MIMPI_ERROR_REMOTE_FINISHED;
        }
        else
        {
            // If we are root, we wait to receive data from root=0, and we
            // save it in recv_data.
            if (MIMPI_World_rank() == root)
                MIMPI_Recv(recv_data, count, 0, FIRST_STAGE_TAG);

            // We forward message from our parent to our sons.
            uint8_t message;
            MIMPI_Recv(&message, sizeof(message), parent, FIRST_STAGE_TAG);

            send_msgs_to_sons(message, left_son, right_son,
                              *(uint8_t *)l_son_data, *(uint8_t *)r_son_data);

            if (message == CANNOT_REDUCE)
                whole_func_retcode = MIMPI_ERROR_REMOTE_FINISHED;
            else
                whole_func_retcode = MIMPI_SUCCESS;
        }
    }
    else
    {
        whole_func_retcode = MIMPI_ERROR_REMOTE_FINISHED;
        // Receiving data from sons was not successful, so we inform parent.
        MIMPI_Retcode ret = inform_parent_about_REDUCE(CANNOT_REDUCE, send_data, count, parent);

        if (ret != MIMPI_SUCCESS)
        {
            send_msgs_to_sons(CANNOT_REDUCE, left_son, right_son,
                              *(uint8_t *)l_son_data, *(uint8_t *)r_son_data);
        }
        else
        {
            uint8_t message;
            MIMPI_Recv(&message, sizeof(message), parent, FIRST_STAGE_TAG);

            send_msgs_to_sons(message, left_son, right_son,
                              *(uint8_t *)l_son_data, *(uint8_t *)r_son_data);
        }
    }

    free(l_son_data);
    free(r_son_data);

    return whole_func_retcode;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root)
{
    if (root >= MIMPI_World_size() || root < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;

    int my_rank = MIMPI_World_rank();

    if (my_rank == 0)
    {
        return real_root_REDUCE(send_data, recv_data, count, op, root);
    }
    else
    {
        return other_proc_REDUCE(send_data, recv_data, count, op, root);
    }

    return MIMPI_SUCCESS;
}