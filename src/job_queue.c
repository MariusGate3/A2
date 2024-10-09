#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "job_queue.h"

int job_queue_init(struct job_queue *job_queue, int capacity) {

  job_queue->jobs = malloc(capacity * (sizeof(void*)));
  if (job_queue->jobs == NULL) {
    return 1;
  }
  pthread_mutex_init(&job_queue->mutex, NULL);
  pthread_cond_init(&job_queue->not_full, NULL);
  pthread_cond_init(&job_queue->not_empty, NULL);
  pthread_cond_init(&job_queue->empty, NULL);
  job_queue->capacity = capacity;
  job_queue->job_counter = 0;
  job_queue->front = 0;

  return 0;
}

int job_queue_destroy(struct job_queue *job_queue) {
  pthread_mutex_lock(&job_queue->mutex);
  while (job_queue->job_counter > 0)
  {
    pthread_cond_wait(&job_queue->empty, &job_queue->mutex);
  }
  free(job_queue->jobs);
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
  assert(pthread_mutex_lock(&job_queue->mutex) == 0);

  // In the case where the queue is full, we dont want to keep the mutex locked.
  // therefor we use a waiting condition:
  while (job_queue->job_counter == job_queue->capacity)
  {
    pthread_cond_wait(&job_queue->not_full, &job_queue->mutex);
  }

  job_queue->jobs[job_queue->job_counter++] = data;

  pthread_cond_signal(&job_queue->not_empty);
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;

}

int job_queue_pop(struct job_queue *job_queue, void **data) {
  pthread_mutex_lock(&job_queue->mutex);
  // We want to wait until the queue is not empty, so that there is something to pop
  // We do this by using a wait condition as sO:
  while (job_queue->job_counter == 0)
  {
    pthread_cond_wait(&job_queue->not_empty, &job_queue->mutex);
  }
  *data = job_queue->jobs[job_queue->front++];
  job_queue->job_counter--;
  if (job_queue->job_counter == 0) {
    job_queue->front = 0;
    pthread_cond_signal(&job_queue->empty);
  } else {
    pthread_cond_signal(&job_queue->not_full);
  }
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}
