#include "BufferedInputStream.hh"

namespace avronode {

/**
 * Deconstructor for BufferedInputStream
 * destroy mutex and release the vector buffer
 */
BufferedInputStream::~BufferedInputStream(){
  pthread_mutex_destroy(&lock);
  data_.clear();
}

/**
 * [BufferedInputStream::next description]
 * @param  data [description]
 * @param  len  [description]
 * @return      [description]
 */
bool BufferedInputStream::next(const uint8_t** data, size_t* len) {
  //pthread_mutex_lock(&lock);
  pthread_mutex_lock(&lock);
  pthread_cond_wait( &cond, &lock);
  int n = data_.size();

  *data = data_.data() + cur_;
  *len = (n - cur_);
  cur_ = n;


  pthread_mutex_unlock(&lock);

  return true;
}

/**
 * decrements the internal buffer index by len. 
 * @param len [description]
 */
void BufferedInputStream::backup(size_t len) {
  cur_ -= len;
}

/**
 * [BufferedInputStream::append description]
 * @param in  [description]
 * @param len [description]
 */
void BufferedInputStream::append(uint8_t* in , int len) {
  pthread_mutex_lock( &lock);
  data_.insert(data_.end(), in, in+len);
  if(len != 0){
    pthread_cond_signal( &cond );
  }
  pthread_mutex_unlock( &lock);
}

/**
 * skips len number of bytes in the internal buffer.
 * @param len [description]
 */
void BufferedInputStream::skip(size_t len) {
  printf("skip count\n");
}

/**
 * returns the current number of bytes read off the internal buffer. 
 * @return [description]
 */
size_t BufferedInputStream::byteCount() const {
  printf("byte count\n");
  return cur_;
}

} // namespace avronode
