#include "BufferedInputStream.hh"

namespace avronode {

/**
 * Deconstructor for BufferedInputStream
 * destroy mutex and release the vector buffer
 */
BufferedInputStream::~BufferedInputStream(){
}

/**
 * [BufferedInputStream::next description]
 * @param  data [description]
 * @param  len  [description]
 * @return      [description]
 */
bool BufferedInputStream::next(const uint8_t** data, size_t* len) {
  pthread_mutex_lock(&lock);
  if(data_.totalLength == 0){
    pthread_cond_wait( &cond, &lock);
    if(!read_){
      return false;
    }
  }
  //*len = 1 ;
  //data_.readData(const_cast<uint8_t**>(data),0,1);
  *len = data_.readBlock(const_cast<uint8_t**>(data));

  pthread_mutex_unlock(&lock);
  return true;
}

/**
 * 
 */
long BufferedInputStream::size(){
  return data_.totalLength;
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
  //printf(" we're writting\n");
  pthread_mutex_lock( &lock);
  data_.appendData(in, 0, len);

  pthread_cond_signal( &cond );
  
  pthread_mutex_unlock( &lock);
}

void BufferedInputStream::close(){
  pthread_mutex_lock( &lock);
  read_ = false;

  pthread_cond_signal( &cond );
  
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
  return cur_;
}

} // namespace avronode
