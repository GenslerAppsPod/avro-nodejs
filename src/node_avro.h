#include <v8.h>
#include <node.h>
#include <node_buffer.h>
#include <avro/ValidSchema.hh>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <queue>
#include "BufferedInputStream.hh"


namespace node {

using namespace node;

struct datumBaton {
  avro::ValidSchema schema;
  avro::GenericDatum *datum;
  const char* errorMessage;
  v8::Persistent<v8::Value> onSuccess;
  v8::Persistent<v8::Value> onError;
};

class Avro : public ObjectWrap
{
public:
  Avro() : ObjectWrap(){};
  ~Avro() {};
  //Buffer *buffer_;
  std::queue<datumBaton> processQueue_;
  std::vector<datumBaton> datums_;
  avro::DecoderPtr decoder_;
  uv_sem_t sem_;
  uv_loop_t *avro_loop_;
  uv_async_t async_;
  uv_mutex_t datumLock_;
  uv_mutex_t queueLock_;
  avronode::BufferedInputStream *buffer_;
  bool read_;
  static void Initialize(v8::Handle<v8::Object> target);
private: 
  static v8::Handle<v8::Value> New(const v8::Arguments& args);
  static v8::Handle<v8::Value> QueueSchema(const v8::Arguments &args);
  static v8::Handle<v8::Value> PendingSchemas(const v8::Arguments &args);
  static v8::Handle<v8::Value> Push(const v8::Arguments &args);  
  static v8::Handle<v8::Value> BufferLength(const v8::Arguments &args);
  static v8::Handle<v8::Value> DecodeFile(const v8::Arguments &args);
  static v8::Handle<v8::Value> EncodeFile(const v8::Arguments &args);
  static v8::Handle<v8::Value> EncodeDatum(const v8::Arguments &args);
  static v8::Handle<v8::Value> DecodeDatum(const v8::Arguments &args);
  static v8::Handle<v8::Value> Close(const v8::Arguments &args);

};
}