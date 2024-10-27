#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "job_queue.h"

int job_queue_init(struct job_queue *job_queue, int capacity) {

  job_queue->jobs = malloc(capacity * (sizeof(void*)));
  if (job_queue->jobs == NULL) {
    return -1;
  }
  pthread_mutex_init(&job_queue->mutex, NULL);
  pthread_cond_init(&job_queue->not_full, NULL);
  pthread_cond_init(&job_queue->not_empty, NULL);
  pthread_cond_init(&job_queue->empty, NULL);
  job_queue->capacity = capacity;
  job_queue->rear = 0;
  job_queue->front = 0;
  job_queue->terminate = 0;

  return 0;
}

int job_queue_destroy(struct job_queue *job_queue) {
  // LOCK
  pthread_mutex_lock(&job_queue->mutex);

  job_queue->terminate = 1;
  // call out to push and pop, to see if they are still missing any jobs
  // if not, they unlock their mutex and return -1
  pthread_cond_broadcast(&job_queue->not_empty);
  pthread_cond_broadcast(&job_queue->not_full);

  while (job_queue->rear != job_queue->front) {
    pthread_cond_wait(&job_queue->empty, &job_queue->mutex);
  }

  free(job_queue->jobs);
  
  // UNLOCK
  pthread_mutex_unlock(&job_queue->mutex);

  pthread_mutex_destroy(&job_queue->mutex);
  pthread_cond_destroy(&job_queue->not_full);
  pthread_cond_destroy(&job_queue->not_empty);
  pthread_cond_destroy(&job_queue->empty);
  return 0;
}

int job_queue_push(struct job_queue *job_queue, void *data) {
  // if mutex is already locked, then the current thread just waits until
  // the mutex is unlocked so that it can continue pushing its job to the queue.
  // LOCK
  pthread_mutex_lock(&job_queue->mutex);

  // in the case where the queue is full, we dont want to keep the mutex locked.
  // therefor we use a waiting condition:
  while (job_queue->rear == job_queue->capacity && job_queue->terminate == 0) {
    pthread_cond_wait(&job_queue->not_full, &job_queue->mutex);
  }

  // check if queue has been terminated
  if (job_queue->terminate != 0) {
    pthread_mutex_unlock(&job_queue->mutex);
    return -1;
  }

  job_queue->jobs[job_queue->rear] = data;
  job_queue->rear = (job_queue->rear + 1) % (job_queue->capacity);

  // signal that the que isnt empty, so the queue_pop function is allowed to pop
  pthread_cond_signal(&job_queue->not_empty);

  // UNLOCK
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}

int job_queue_pop(struct job_queue *job_queue, void **data) {
  // LOCK
  pthread_mutex_lock(&job_queue->mutex);

  // we want to wait until the queue is not empty, so that there is something to pop
  // we do this by using a wait condition as so:
  while (job_queue->rear == job_queue->front && job_queue->terminate == 0) {
    pthread_cond_wait(&job_queue->not_empty, &job_queue->mutex);
  }

  // check if queue has been terminated
  if (job_queue->rear == job_queue->front && job_queue->terminate != 0) {
    pthread_mutex_unlock(&job_queue->mutex);
    return -1;
  }

  *data = job_queue->jobs[job_queue->front];
  job_queue->front = (job_queue->front + 1) % job_queue->capacity;
  // signal the the queue isn't full, so the queue_push function is allowed
  // to push another job
  pthread_cond_signal(&job_queue->not_full);

  if (job_queue->rear == job_queue->front) {
    // signal that the queue is empty, so the queue_destroy function is allowed
    // to destroy the queue
    pthread_cond_signal(&job_queue->empty);
  }

  // UNLOCK
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}
