#include <avro/Stream.hh>
#include <semaphore.h>
#include <signal.h>
#include <stdlib.h>
#include <pthread.h>
#include <iostream>
#include <fstream>
#include <v8.h>
#include <node.h>
#include "DynamicBuffer.hh"

namespace avronode {
using namespace node;

class BufferedInputStream : public avro::InputStream
{
  DynamicBuffer data_;
  const size_t chunkSize_;
  size_t size_;
  bool hasData_;
  size_t available_;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  uv_sem_t sem_;
  size_t cur_;
  bool read_;

public:
  BufferedInputStream(std::vector<uint8_t> b,
    size_t chunkSize, size_t available) :
    data_(), chunkSize_(chunkSize),hasData_(false), read_(true),
    available_(available), cur_(0) {
      pthread_mutex_init(&lock, NULL);
      pthread_cond_init (&cond, NULL);
    }
  ~BufferedInputStream();
    
  bool next(const uint8_t** data, size_t* len) ;

  void backup(size_t len);
  void append(uint8_t* in, int len);

  void skip(size_t len);

  void close();

  long size();

  size_t byteCount() const;

};

}