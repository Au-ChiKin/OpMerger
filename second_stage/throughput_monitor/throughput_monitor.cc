/******************************************************************
 * The Main program with the two functions. A simple
 * example of creating and using a thread is provided.
 ******************************************************************/

#include "helper.h"

// Error list
#define NOT_ENOUGH_PARAMETERS 1
#define INVALID_PARAMETER 2
#define CREATE_SEMAPHORE_FAIL 3

// Semaphore for mutal excluisive
static const short unsigned MUTEX = 0;
// Semaphore for space in the queue
static const short unsigned SPACE = 1;
// Semaphore for items in the queue
static const short unsigned ITEM = 2;
// Semaphore for producers and consumer initialisation
static const short unsigned INIT = 3; 

static const int MAX_DURATION = 10; // 1-10s
static const int MAX_JOB_GAP = 5; // 1-5s 

/**
 * Data structure for a job, storing job id and duration of consuming a 
 * job in seconds
 */ 
struct JobInfo {
  int id;
  int duration;

  JobInfo(int _id, int _duration) :
    id(_id),
    duration(_duration)
  {
  }
};

/**
 * Data structure for parameters passed to a producer
 */ 
struct ProducerParam {
  int local_id;
  int job_num;
  int sem_id;
  deque<JobInfo *> * job_queue; 
  deque<int> * id_queue;
};

/**
 * Data structure for parameters passed to a consumer
 */ 
struct ConsumerParam {
  int local_id;
  int sem_id;
  deque<JobInfo *> * job_queue;
  deque<int> * id_queue;
};

/**
 * Producer thread function. Produce a job and add it to the end of the queue.
 */ 
void *producer (void *id);

/**
 * Consumer thread funciton. Fetch a job from the front of the queue and consume
 * the job. 
 */ 
void *consumer (void *id);

/**
 * Check whether the input arguements are valid and store them into an array 
 * int args [4]
 */ 
void input_arg(int argc, char **argv, int * args);

/**
 * Add a job with the passed duration to the back of job_queue. The id of new job 
 * is stored in the head id_queue, which is then returned.
 */ 
int job_queue_add(deque<JobInfo *> *  job_queue, int duration, deque<int> * id_queue);

/**
 * Fetch a job in the end of job_queue. The duration of job is stored to duration 
 * and id is push back to id_queue before being returned. 
 */ 
int job_queue_fetch(deque<JobInfo *> *  job_queue, int &duration, deque<int> * id_queue);



/* We should actually use this main to be the main function of the OpMerger or add similar content to the current main */

int main (int argc, char **argv) {
  int queue_size = -1; // (Should be removed) There should be no restriction on the event on the queue
  int job_num = -1;  // (Should be removed) Each producer produces the same number of jobs
  int producer_num = 1; // query
  int consumer_num = 1; // 
  int args [4];
  key_t key;
  int sem_id;
  pthread_t * producer_ids, * consumer_ids;

  /* read in fout command line parameters */
//   input_arg(argc, argv, args);
//   queue_size = args[0];
//   job_num = args[1];
//   producer_num = args[2];
//   consumer_num = args[3]; 
//   cerr << "Read in parameters successful" << endl; 

  /* Initialise data structures and variables */
  deque<JobInfo *> job_queue;
  deque<int> id_queue;
  for (int id = 0; id < queue_size; id++) {
    id_queue.push_back(id);
  }
  producer_ids = new pthread_t [producer_num];
  consumer_ids = new pthread_t [consumer_num];

  /* Set-up and initialise semaphores, as necessary */ 
  key = SEM_KEY;
  sem_id = sem_create(key, 4);
  if (sem_id == -1) {
    cerr << "Error: There is an existed semaphore array with the same id" << endl;
    delete [] producer_ids;
    delete [] consumer_ids;
    return CREATE_SEMAPHORE_FAIL; 
  }
  
  sem_init(sem_id, MUTEX, 1); 
  sem_init(sem_id, SPACE, queue_size);
  sem_init(sem_id, ITEM, 0); 
  sem_init(sem_id, INIT, 0); 

  /* Create the required producers and consumers */
  for (int producer_i = 0; producer_i < producer_num; producer_i++) {
    ProducerParam parameter = {producer_i,
                               job_num, 
                               sem_id,  
                               &job_queue,
                               &id_queue}; 
    pthread_create(&producer_ids[producer_i], NULL, producer, (void *) &parameter);
    if (errno != 0) {
      perror("Failed to create a producer thread");
      delete [] producer_ids;
      delete [] consumer_ids;	      
      pthread_exit(&errno);
    }
    sem_wait(sem_id, INIT); //  wait for the producer to finish initialisation
  }
  for (int consumer_i = 0; consumer_i < consumer_num; consumer_i++) {
    ConsumerParam parameter = {consumer_i,
                               sem_id,
                               &job_queue,
                               &id_queue}; 
    pthread_create(&consumer_ids[consumer_i], NULL, consumer, (void *) &parameter);
    if (errno != 0) {
      perror("Failed to create a consumer thread");
      delete [] producer_ids;
      delete [] consumer_ids;	      
      pthread_exit(&errno);
    }
    sem_wait(sem_id, INIT);
  }

  /* Quit but ensure that there is process clean-up */
  for (int producer_i = 0; producer_i < producer_num; producer_i++) {
    pthread_join(producer_ids[producer_i], nullptr);
    if (errno != 0) {
      perror("Fail to join main thread and producer thread");
      delete [] producer_ids;
      delete [] consumer_ids;	      
      pthread_exit(&errno);
    }
  }
  for (int consumer_i = 0; consumer_i < consumer_num; consumer_i++) {
    pthread_join(consumer_ids[consumer_i], nullptr);
    if (errno != 0) {
      perror("Failed to join main thread and consumer thread");
      delete [] producer_ids;
      delete [] consumer_ids;	      
      pthread_exit(&errno);
    }
  }
  delete [] producer_ids;
  delete [] consumer_ids;
  sem_close(sem_id);
  
  return 0;
}

void *producer (void *parameter) {
  /* Initialise parameter */ 
  int job_num = ((ProducerParam *) parameter)->job_num;
  int sem_id = ((ProducerParam *) parameter)->sem_id; 
  int id = ((ProducerParam *) parameter)->local_id; 
  deque<JobInfo *> * job_queue = ((ProducerParam *) parameter)->job_queue; 
  deque<int> * id_queue = ((ProducerParam *) parameter)->id_queue;

  cerr << "Producer(" << id << ") initialised." << endl;
  sem_signal(sem_id, INIT); 

  /* Add jobs and print status*/ 
  while (job_num > 0) {
    int job_id;
    int duration = rand() % MAX_DURATION + 1;

    sem_wait_20s(sem_id, SPACE);
    if (errno == EAGAIN) {
      cerr << "Producer(" << id+1 << "): No space to add a job so exitted" << endl;
      pthread_exit(&errno);      
    }
    sem_wait(sem_id, MUTEX);
    job_id = job_queue_add(job_queue, duration, id_queue);
    sem_signal(sem_id, MUTEX);
    sem_signal(sem_id, ITEM);

    cout << "Producer(" << id+1 << "): Job id "<< job_id+1 
         << " duration " << duration << endl;
    sleep(rand() % MAX_JOB_GAP + 1);
    job_num--; 
  }

  /* Quit */ 
  cout << "Producer(" << id+1 << "): No more jobs to generate." << endl;
  pthread_exit(0); 
}

void *consumer (void *parameter) {
  /* Initialise parameter */ 
  int id = ((ConsumerParam *) parameter)->local_id; 
  int sem_id = ((ConsumerParam *) parameter)->sem_id; 
  deque<JobInfo *> * job_queue = ((ConsumerParam *) parameter)->job_queue;
  deque<int> * id_queue = ((ProducerParam *) parameter)->id_queue;

  cerr << "Consumer(" << id+1 << ") initialised." << endl;
  sem_signal(sem_id, INIT); 

  /* Consume jobs */ 
  while (1) {
    int duration; 
    int job_id; 

    sem_wait_20s(sem_id, ITEM);
    if (errno == EAGAIN) {
      cout << "Consumer(" << id+1 << "): No more jobs left." << endl;
      pthread_exit(&errno);
    }
    sem_wait(sem_id, MUTEX);
    job_id = job_queue_fetch(job_queue, duration, id_queue);
    sem_signal(sem_id, MUTEX);
    sem_signal(sem_id, SPACE);

    cout << "Consumer(" << id+1 << "): Job id " << job_id+1 
         << " executing sleep duration " << duration << endl; 
    sleep(duration);
    cout << "Consumer(" << id+1 << "): Job id " << job_id+1
         << " completed" << endl;
  }

  pthread_exit (0);
}

void input_arg(int argc, char **argv, int * args) {
  if (argc < 5) {
    cerr << "Error: Not enough arguements" << endl; 
    exit(NOT_ENOUGH_PARAMETERS); 
  }
  for (int arg_i=0; arg_i < argc-1; arg_i++) {
    args[arg_i] = check_arg(argv[arg_i+1]);
    cerr << args[arg_i] << endl; 
    if (args[arg_i] == -1) {
      cerr << "Error: Invalid arguements" << endl; 
      exit(INVALID_PARAMETER);  // Before creating any thread so used exit
    }
  }
}

int job_queue_add(deque<JobInfo *> * job_queue, int duration, deque<int> * id_queue) {
  int job_id = id_queue->front();
  JobInfo * new_job;

  id_queue->pop_front();
  new_job = new JobInfo(job_id, duration);
  job_queue->push_back(new_job);

  return job_id;
}

int job_queue_fetch(deque<JobInfo *> * job_queue, int &duration, deque<int> * id_queue) {
  int job_id;
  JobInfo * front = job_queue->front(); 

  job_id = front->id;
  id_queue->push_back(job_id); 
  duration = front->duration;
  delete(front);
  job_queue->pop_front();

  return job_id;
}
