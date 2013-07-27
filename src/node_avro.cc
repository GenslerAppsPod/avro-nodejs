#include "node_avro.h"
#include "translate.h"

namespace node {


using namespace v8;
using namespace node;
using namespace avro;

Persistent<String> on_schema;
Persistent<String> on_datum;
Persistent<String> on_error;

static void ResultEvent(uv_async_t *handle, int status);
static void Process(uv_work_t* work_req);
static void After(uv_work_t* work_req, int status);
static void OnError(Avro *ctx, Persistent<Value> callback, const char* error);
static void OnSchema(Avro *ctx, const char* schema);
static void OnDatum(Avro *ctx, Persistent<Value> callback, Handle<Value> datum);
void handleCallbacks(Avro *ctx, datumBaton *baton, const Arguments &args, int startPos);

/**
 * Constructs a new avro object. Creates new worker thread and async callbacks
 * for async support.
 */
Handle<Value> Avro::New(const Arguments& args){
    HandleScope scope;

    Avro *ctx = new Avro();
    ctx->decoder_= binaryDecoder();
    std::vector<uint8_t> data;
    ctx->buffer_ = new avronode::BufferedInputStream(data, 0, 0);
    ctx->read_  = true;
    ctx->avro_loop_ = uv_default_loop();
    ctx->Wrap(args.This());
    uv_sem_init(&ctx->sem_, 0);
    uv_mutex_init(&ctx->datumLock_);
    uv_mutex_init(&ctx->queueLock_);

    uv_async_init(ctx->avro_loop_, &ctx->async_, ResultEvent);
    //create new work_t
    uv_work_t *work_req = new uv_work_t;

    work_req->data = ctx;

    uv_queue_work(ctx->avro_loop_,
                  work_req,
                  Process,
                  After);  

    return args.This();
}

/**
 * [ResultEvent description]
 * @param handle [The uv_async handle ]
 * @param status [description]
 */
static void ResultEvent(uv_async_t *handle, int status) {
  Avro* ctx = (Avro *)handle->data;
  // loop through datums then release lock.
  // Thread safe block here 
  // ---------------------------------------------------------------------
  uv_mutex_lock(&ctx->datumLock_);
  for(size_t i = 0;i < ctx->datums_.size();i++){
    datumBaton baton = ctx->datums_[i];
    //test to see if we have an error;
    if(!strlen(baton.errorMessage)){
      //success
      OnDatum(ctx, baton.onSuccess, DecodeAvro(*baton.datum));
    }else{
      //print error message
      printf("baton error '%s' length %d\n",baton.errorMessage, strlen(baton.errorMessage));
      OnError(ctx, baton.onError, baton.errorMessage);
    }

    delete baton.datum;
  }
  ctx->datums_.clear();
  uv_mutex_unlock(&ctx->datumLock_);

  // Thread safe block ends here
  // ---------------------------------------------------------------------
}

/**
 * [Closes the avro object. ]
 */
Handle<Value> Avro::Close(const Arguments &args){
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());
  uv_mutex_lock(&ctx->queueLock_);
  while(!ctx->processQueue_.empty()){
    ctx->processQueue_.pop();
  }
  ctx->read_ = false;
  ctx->buffer_->close();

  //set the smart pointer to 0 so that it can be cleaned up.
  ctx->decoder_.reset();
  uv_sem_post(&ctx->sem_);
  uv_mutex_unlock(&ctx->queueLock_);
  
  //uv_loop_delete(ctx->avro_loop_);
  uv_run(ctx->avro_loop_, UV_RUN_DEFAULT); 
  return scope.Close(Undefined());
}

/**
 * Add a schema onto the schema queue. This is then processes 
 * in the loop
 */
Handle<Value> Avro::QueueSchema(const Arguments &args){
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());
  datumBaton baton;
  baton.errorMessage = "";

  if(!args[0]->IsString()){
    OnError(ctx, on_error, "schema must be a string");
    return scope.Close(Undefined());
  }
  //grab string from input.
  v8::String::Utf8Value schemaString(args[0]->ToString());
  std::istringstream is(*schemaString);

  try{
    compileJsonSchema(is, baton.schema);

    // if args > 2 and args[1] is a function set our onSuccess  
    handleCallbacks(ctx, &baton, args, 1);

    // ------------------------------------------------
    // lock section here adding to queue
    uv_mutex_lock(&ctx->queueLock_);

    //push new schema to queue
    ctx->processQueue_.push(baton);
    uv_sem_post(&ctx->sem_);
    uv_mutex_unlock(&ctx->queueLock_);
    // ------------------------------------------------
    // release lock 
    // 
  }catch(std::exception &e){
    //TODO should send back the bad schema for user reference. 
    OnError(ctx, on_error, e.what());
    return scope.Close(Undefined());
  }
  return scope.Close(Undefined());
}


/**
 * Gets the number of schemas that are queued.  
 */
 Handle<Value> Avro::PendingSchemas(const Arguments &args){
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());

  return scope.Close(Number::New(ctx->processQueue_.size()));
 }

/**
 * Sending bytes to the decode async thread loop.
 * @param bytes [A Buffer containing a series of bytes.]
 * @return [a javascript undefined]
 */
Handle<Value> Avro::Push(const Arguments &args){
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());

  if (args.Length() > 1) {
    OnError(ctx, on_error, "Wrong number of arguments");
    return scope.Close(Undefined());
  }
  try{
    if(args[0]->IsObject()){
      Local<Object> in_buf = args[0]->ToObject();
      //get length of buffer
      int len = Buffer::Length(in_buf);
      //get data of the buffer
      uint8_t *in = reinterpret_cast<uint8_t*>(Buffer::Data(in_buf));

      // ------------------------------------------------
      // lock section here adding to BufferedInputStream.
      //uv_mutex_lock(&ctx->datumLock_);

      ctx->buffer_->append(in,len);

      //uv_mutex_unlock(&ctx->datumLock_); 
      // release lock section here.      
      // ------------------------------------------------
    }else{
      OnError(ctx, on_error, "Argument must be a Byte Array");
    }

  }catch(std::exception &e){
    //TODO should send back the bad schema for user reference. 
    OnError(ctx, on_error, e.what());
    return scope.Close(Undefined());
  }


  return scope.Close(Undefined());
}


/**
 * Gets the size of the underlining Buffer.
 */
Handle<Value> Avro::BufferLength(const Arguments &args){
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());

  return scope.Close(Number::New(ctx->buffer_->size()));
   
}

/**
 * Takes a avro data file that must contain the schema definition
 * as part of the file. For each datum that is parsed out of the file
 * OnDatum will be called. 
 * @param avrofile [The file containing the Avro data and schema. ]
 */
Handle<Value> Avro::DecodeFile(const Arguments &args) {  
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());

  if (args.Length() < 1) {
    ThrowException(v8::Exception::TypeError(String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }
  datumBaton baton;
    // if args > 2 and args[1] is a function set our onSuccess  
  handleCallbacks(ctx, &baton, args, 1);

  if(args[0]->IsString()){
    // get the param
    v8::String::Utf8Value filename(args[0]->ToString());

    try{
      avro::DataFileReader<avro::GenericDatum> dfr(*filename);
      ValidSchema schema = dfr.dataSchema();
      std::ostringstream oss(std::ios_base::out);
      avro::GenericDatum datum(dfr.dataSchema());

      while(dfr.read(datum)){
        OnDatum(ctx, baton.onSuccess, DecodeAvro(datum));
      }
      
      if(baton.onSuccess->IsFunction()){
        baton.onSuccess.Dispose();
      }
    }catch(std::exception &e){
      OnError(ctx, baton.onError, e.what());
    }
  }else{
    OnError(ctx, on_error, "Wrong Argument. Must be string for filename");
  }

  return scope.Close(Undefined());
}
/**
 * The sync version of decode datum. The buffer of bytes provided must 
 * contain the entire datum to be decoded. Otherwise an error will be thrown.
 * @param schema [The schema to decode bytes with.]
 * @param bytes [A node Buffer object containing all of the bytes of the datum.]
 */
Handle<Value> Avro::DecodeDatum(const Arguments &args){
  HandleScope scope;
  if(args.Length() != 2){
    ThrowException(v8::Exception::TypeError(String::New("Wrong number of arguments")));
    return scope.Close(Undefined());        
  }

  // throws error if there is no Buffer instance for 
  // args[1].
  if(args[0]->IsString()&&Buffer::HasInstance(args[1])){
    //create schema from string
    v8::String::Utf8Value schemaString(args[0]->ToString());
    std::istringstream is(*schemaString);

    ValidSchema schema;
    compileJsonSchema(is, schema);

    DecoderPtr decoder = binaryDecoder();
    Local<Object> in_buf = args[1]->ToObject();
    int len = Buffer::Length(in_buf);
    uint8_t *in = reinterpret_cast<uint8_t*>(Buffer::Data(in_buf));

    std::auto_ptr<avro::InputStream> inputStream = memoryInputStream(in,len);
    decoder->init(*inputStream);

    //create reader and generic datum.
    GenericReader reader(schema, decoder);
    GenericDatum *datum = new GenericDatum(schema);
    reader.read(*datum);
    Handle<Value> datumObject = DecodeAvro(*datum);
    decoder.reset();
    delete datum;

    return scope.Close(datumObject);
  }

  return scope.Close(Undefined());
}

/**
 * Encodes data to a file specified.
 *
 * @param filename [The file to write to]
 * @param schema [The schema to encode the data with.]
 * @param data [The data that is going to be encoded with specified schema.]
 */
Handle<Value> Avro::EncodeFile(const Arguments &args) { 
  HandleScope scope; 
  /*
  if(args[0]->IsString()){
    // get the param
    v8::String::Utf8Value filename(args[0]->ToString());

    std::ifstream ifs("cpx.json");

    avro::ValidSchema cpxSchema;
    avro::compileJsonSchema(ifs, cpxSchema);

    std::auto_ptr<avro::OutputStream> out = memoryOutputStream(1);
    avro::EncoderPtr e = binaryEncoder();
    e->init(*out);

    for(int i = 0;i<1;i++){
      c::cpx c1;
      c1.re.set_double(10*i);
      c1.im = 105;
      encode(*e,c1);
    }

    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(*out);

    StreamReader reader(*in);
    std::ofstream myFile ("data.bin", std::ios::out | std::ios::binary);
    while(reader.hasMore()){
      myFile.put(reader.read());
    }
    //for some reason calling myFile.flush() was 
    //causeing the un defined buffer of reader or in 
    // to be written causing all sorts of problems. 
    myFile.close();
  }
  */
  return scope.Close(Undefined());
}

/**
 * A synchronous function to encode a datum into avro binary
 * @param schema [The schema to encode the datum with]
 * @param datum [The datum to be encoded]
 */
Handle<Value> Avro::EncodeDatum(const Arguments &args){
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());

  if(args.Length()< 2){
    OnError(ctx, on_error, "EncodeDatum requires two params");
    return scope.Close(Array::New());
  }

  if(!args[0]->IsString()){
    OnError(ctx, on_error, "schema must be a string");
    return scope.Close(Array::New());
  }

  Local<Array> byteArray = Array::New();
  v8::String::Utf8Value schemaString(args[0]->ToString());
  Local<Value> object = args[1];
  ValidSchema schema;

  try{
    //grab string from input.

    std::istringstream is(*schemaString);
    //create schema
    //
    compileJsonSchema(is, schema);

    avro::GenericDatum datum(schema);
    datum = DecodeV8(datum, object);

    std::auto_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::validatingEncoder(schema, avro::binaryEncoder());

    e->init(*out);

    avro::encode(*e, datum);

    //need to flush the bytes to the stream (aka out);
    e->flush();

    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(*out);

    avro::StreamReader reader(*in);
    int i = 0;
    // could initialize a buffer and then do a while read of x chunk say 4k
    while(reader.hasMore()){
      byteArray->Set(i, Uint32::New(reader.read()));
      i++;
    }
  }catch(std::exception &e){
    std::string error = e.what();
    std::string errorMessage = error + *schemaString + "\n";
    OnError(ctx, on_error, errorMessage.c_str());
    return scope.Close(Array::New());
  }

  //construct a byte array to send back to the javascript

  return scope.Close(byteArray);
}

/**
 * [Internal thread to process avro decode bytes in a none
 * blocking IO manor.]
 * @param work_req [description]
 */
static void Process(uv_work_t* work_req){
  Avro* ctx = (Avro *)work_req->data;
  ctx->decoder_->init(*(ctx->buffer_));

  while(true){
    uv_sem_wait(&ctx->sem_);
    //if we got a signal to end and we have an empty queue break;
    if(!ctx->read_){
      break;
    }
    uv_mutex_lock(&ctx->queueLock_);
      datumBaton baton = ctx->processQueue_.front();
      ctx->processQueue_.pop();
    uv_mutex_unlock(&ctx->queueLock_);

    try{
      //create reader and generic datum.
      GenericReader reader(baton.schema, ctx->decoder_);
      baton.datum = new GenericDatum(baton.schema);

      //This is a blocking read
      reader.read(*baton.datum);

    }catch(std::exception &e){
      printf("we got an error in the process\n");
      baton.errorMessage = e.what();
    }
        // Thread safe area here
    // ---------------------------------------------------------------
    // mutex lock for writing to datum on avro object. 
    // 
    uv_mutex_lock(&ctx->datumLock_);

    ctx->datums_.push_back(baton);

    uv_mutex_unlock(&ctx->datumLock_);
    //Thread safe area leave
    // ---------------------------------------------------------------
    //Send data to returning javascript callback
    ctx->async_.data = (void*) ctx;
    uv_async_send(&ctx->async_);

  }
}

/**
 * This is the function that is called directly after the Process function
 * is finished. Currently it runs the entire lifetime of Avro instance. 
 * @param work_req [description]
 * @param status   [description]
 */
static void After(uv_work_t* work_req, int status){
  HandleScope scope;

  Avro *ctx = static_cast<Avro*>(work_req->data);
  uv_close((uv_handle_t*)&ctx->async_, NULL);
  free(work_req);

}


/**
 * Parses out the two callbacks for the arguments. It is assumed that they are 
 * defined in pairs. OnSuccess, OnError.  
 * @param ctx              [Our Avro Object]
 * @param baton            [the baton we're setting the callbacks on]
 * @param args             [The arguments]
 * @param startingPosition [The starting position to look for the callbacks in the Arugments]
 */
void handleCallbacks(Avro *ctx, datumBaton *baton, const Arguments &args, int startPos){
  if(args.Length() > startPos+1 ){
    if(args[startPos]->IsFunction()){
      baton->onSuccess = Persistent<Function>::New(Handle<Function>::Cast(args[startPos]));
    }else{
      //error onSuccess must be a function
      OnError(ctx, on_error, "onSuccess must be a callback function.");
    }
  }else{
    baton->onSuccess = on_datum;
  }
  startPos++;
  // set onerror callback for the proccess struct 
  if(args.Length() > startPos+1 ){
    if(args[startPos]->IsFunction()){
      baton->onError = Persistent<Function>::New(Handle<Function>::Cast(args[startPos]));
    }else{
      OnError(ctx, on_error, "onError must be a callback function.");
    }
    //error onSuccess must be a function
  }else{
    baton->onError = on_error;
  }
}

/**
 * [returns the error message to the callback specified.]
 * @param ctx [The avro object that we get the handle from]
 * @param callback [The call back that we return the data error message to]
 * @param error [error message]
 * 
 */
static void OnError(Avro *ctx, Persistent<Value> callback, const char* error){

  Local<Value> args[1] = { String::New(error) };
  if(callback->IsFunction()){
    MakeCallback(ctx->handle_, Persistent<Function>::Cast(callback), 1, args);
    //make sure to remove the callback. Actually don't do that here. 
    //callback.Dispose();
  }else if(callback->IsString()){
    Local<Value> callback_v = ctx->handle_->Get(callback);
    //if there is no error callback defined we'll just throw an exception;
    if(callback_v->IsFunction()){
      MakeCallback(ctx->handle_, Local<Function>::Cast(callback_v), 1, args);
    }else{
      v8::ThrowException(v8::String::New(error));
    }

  }else{
    printf("error wtf\n"); 
  }
}

/**
 * [Returns the schema definiton in json format]
 * @param ctx [The context for our callback]
 * @param schema [The scheam that we set.]
 */
static void OnSchema(Avro *ctx, const char* schema){
  HandleScope scope;

  assert(ctx->handle_->Get(on_schema)->IsFunction() && "Invalid schema handler");

  Local<Value> args[1] = { String::New(schema) };

  MakeCallback(ctx->handle_, on_schema, 1, args);
}

/**
 * [OnDatum return the datum to the specified callback method]
 * @param ctx [The avro object that we get the handle from]
 * @param callback [The call back that we return the data error message to]
 * @param datum [The datum that we're passing to the callback function]
 */
static void OnDatum(Avro *ctx, Persistent<Value> callback, Handle<Value> datum) {

  Local<Value> args[1] = { Local<Value>::New(datum) };

  if(callback->IsFunction()){
    MakeCallback(ctx->handle_, Persistent<Function>::Cast(callback), 1, args);
    //callback.Dispose();
  }else if(callback->IsString()){
    MakeCallback(ctx->handle_, Persistent<String>::Cast(callback), 1, args);
  }else{
    printf("datum wtf\n"); 
  }
}

/**
 * [Creates all of the possible calls from the javascript]
 * @param target [description]
 */
void Avro::Initialize(Handle<Object> target){
  HandleScope scope;

  Local<FunctionTemplate> a_temp = FunctionTemplate::New(Avro::New);

  a_temp->InstanceTemplate()->SetInternalFieldCount(1);

  NODE_SET_PROTOTYPE_METHOD(a_temp, "decodeFile", Avro::DecodeFile);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "encodeFile", Avro::EncodeFile);

  NODE_SET_PROTOTYPE_METHOD(a_temp, "push", Avro::Push);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "bufferLength", Avro::BufferLength);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "queueSchema", Avro::QueueSchema);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "pendingSchemas", Avro::PendingSchemas);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "decodeDatum", Avro::DecodeDatum);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "encodeDatum", Avro::EncodeDatum);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "close", Avro::Close);
  a_temp->SetClassName(String::NewSymbol("Avro"));
  target->Set(String::NewSymbol("Avro"), a_temp->GetFunction());

  on_schema = NODE_PSYMBOL("onschema");
  on_datum = NODE_PSYMBOL("ondatum");
  on_error = NODE_PSYMBOL("onerror");

}

NODE_MODULE(avro, Avro::Initialize);

}