#include "node_avro.h"
#include "translate.h"

namespace node {


using namespace v8;
using namespace node;
using namespace avro;

Persistent<String> on_schema;
Persistent<String> on_datum;
Persistent<String> on_error;

uv_async_t async;

static void PrintResult(uv_async_t *handle, int status);
static void Process(uv_work_t* work_req);
static void After(uv_work_t* work_req, int status);
static void OnError(Avro *ctx, Persistent<Value> callback, const char* error);
static void OnSchema(Avro *ctx, const char* schema);
static void OnDatum(Avro *ctx, Persistent<Value> callback, Handle<Value> datum);
void handleCallbacks(Avro *ctx, datumBaton *baton, const Arguments &args, int startPos);

Handle<Value> Avro::New(const Arguments& args){
    HandleScope scope;

    Avro *ctx = new Avro();
    ctx->decoder_= binaryDecoder();
    std::vector<uint8_t> data;
    ctx->buffer_ = new avronode::BufferedInputStream(data, 0, 0);

    ctx->Wrap(args.This());
    uv_sem_init(&ctx->sem_, 0);
    uv_mutex_init(&ctx->datumLock_);
    uv_mutex_init(&ctx->queueLock_);

    uv_async_init(uv_default_loop(), &async, PrintResult);
    //create new work_t
    uv_work_t *work_req = new uv_work_t;
    work_req->data = ctx;

    uv_queue_work(uv_default_loop(),
                  work_req,
                  Process,
                  After);  

    return args.This();
}

static void PrintResult(uv_async_t *handle, int status) {
  Avro* ctx = (Avro *)handle->data;
  // loop through datums then release lock.
  // Thread safe block here 
  // ---------------------------------------------------------------------
  uv_mutex_lock(&ctx->datumLock_);
  for(int i = 0;i < ctx->datums_.size();i++){
    datumBaton baton = ctx->datums_[i];
    //test to see if we have an error;
    if(baton.errorMessage.empty()){
      //success
      OnDatum(ctx, baton.onSuccess, DecodeAvro(baton.datum));
    }else{
      //print error message
      OnError(ctx, baton.onError, baton.errorMessage.c_str());
    }
  }
  ctx->datums_.clear();
  uv_mutex_unlock(&ctx->datumLock_);
  // Thread safe block ends here
  // ---------------------------------------------------------------------
}

/**
 * Add a schema onto the schema queue. This is then processes 
 * in the loop
 */
Handle<Value> Avro::QueueSchema(const Arguments &args){
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());

  datumBaton baton;

  if(!args[0]->IsString()){
    OnError(ctx, on_error, "schema must be a string");
    return scope.Close(Undefined());
  }
  //grab string from input.
  v8::String::Utf8Value schemaString(args[0]->ToString());
  std::istringstream is(*schemaString);
  //create schema
  //
  ValidSchema schema;
  try{
    compileJsonSchema(is, schema);
  }catch(std::exception &e){
    //TODO should send back the bad schema for user reference. 
    OnError(ctx, on_error, e.what());
    return scope.Close(Undefined());
  }

  baton.schema = schema;

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

  return scope.Close(Undefined());
}

/**
 * Sending bytes to the decode async thread loop.
 */
Handle<Value> Avro::Push(const Arguments &args){
  HandleScope scope;
  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());

  if (args.Length() > 1) {
    OnError(ctx, on_error, "Wrong number of arguments");
    return scope.Close(Undefined());
  }

  if(args[0]->IsObject()){
    //convert argument to Object
    Local<Object> in_buf = args[0]->ToObject();

    //get length of buffer
    int len = Buffer::Length(in_buf);
    uint8_t *in = reinterpret_cast<uint8_t*>(Buffer::Data(in_buf));

    //get data of the buffer
    // ------------------------------------------------
    // lock section here adding to BufferedInputStream.
    uv_mutex_lock(&ctx->datumLock_);

    ctx->buffer_->append(in,len);

    uv_mutex_unlock(&ctx->datumLock_);      
    // ------------------------------------------------
    // release lock section here. 
    

  }else{
    OnError(ctx, on_error, "Argument must be a Byte Array");
  }

  return scope.Close(Undefined());
}

/**
 * Takes a avro data file that must contain the schema definition
 * as part of the file. For each datum that is parsed out of the file
 * OnDatum will be called. 
 * TODO change this to provide a callback for on datum and on error.
 * @param avrofile
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
      schema.toJson(oss);
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
 * @param schema
 * @param bytes
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
    return scope.Close(DecodeAvro(*datum));
  }

  return scope.Close(Undefined());
}

/**
 * Encodes datum(s) to a file specified.
 *
 * @param filename
 * @param schema
 * @param datums
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
 * Provides a sync function to encode a datum.
 * @param schema
 * @param datum
 */
Handle<Value> Avro::EncodeDatum(const Arguments &args){
  HandleScope scope;

  Avro * ctx = ObjectWrap::Unwrap<Avro>(args.This());

  if(args.Length()< 2){
    OnError(ctx, on_error, "EncodeDatum requires two params");
    return scope.Close(Undefined());
  }

  if(!args[0]->IsString()){
    OnError(ctx, on_error, "schema must be a string");
    return scope.Close(Undefined());
  }
  //grab string from input.
  v8::String::Utf8Value schemaString(args[0]->ToString());
  std::istringstream is(*schemaString);
  //create schema
  //
  ValidSchema schema;
  try{
    compileJsonSchema(is, schema);
  }catch(std::exception &e){
    //TODO should send back the bad schema for user reference. 
    OnError(ctx, on_error, "Error: compiling schema for EncodeDatum");
    return scope.Close(Undefined());
  }

  Local<Value> object = args[1];
  avro::GenericDatum datum(schema);
  datum = DecodeV8(datum, object);

  std::auto_ptr<avro::OutputStream> out = avro::memoryOutputStream();
  avro::EncoderPtr e = avro::validatingEncoder(schema, avro::binaryEncoder());

  e->init(*out);

  try{
    avro::encode(*e, datum);
  }catch(std::exception &e){
    std::string error = e.what();
    std::string errorMessage = error + *schemaString;
    OnError(ctx, on_error, errorMessage.c_str());
    return scope.Close(Array::New());
  }
  //need to flush the bytes to the stream (aka out);
  e->flush();

  std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(*out);

  avro::StreamReader reader(*in);
  int i = 0;
  // could initialize a buffer and then do a while read of x chunk say 4k
  Local<Array> byteArray = Array::New();
  while(reader.hasMore()){
    byteArray->Set(i, Uint32::New(reader.read()));
    i++;
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
    //create reader and generic datum.
    datumBaton baton = ctx->processQueue_.front();
    GenericReader reader(baton.schema, ctx->decoder_);
    GenericDatum *datum = new GenericDatum(baton.schema);
    //disgard the top schema
    
    // Thread safe area here
    // ---------------------------------------------------------------
    uv_mutex_lock(&ctx->queueLock_);
    ctx->processQueue_.pop();
    uv_mutex_unlock(&ctx->queueLock_);
    //Thread safe area leave
    // ---------------------------------------------------------------
    
    //This is a blocking read
    reader.read(*datum);
    baton.datum = *datum;
    // Thread safe area here
    // ---------------------------------------------------------------
    // mutex lock for writing to datum on avro object. 
    uv_mutex_lock(&ctx->datumLock_);
    ctx->datums_.push_back(baton);

    uv_mutex_unlock(&ctx->datumLock_);
    //Thread safe area leave
    // ---------------------------------------------------------------
    //Send data to returning javascript callback
    async.data = (void*) ctx;
    uv_async_send(&async);
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
 * [OnError description]
 * @param error [description]
 */
static void OnError(Avro *ctx, Persistent<Value> callback, const char* error){

  Local<Value> args[1] = { String::New(error) };

  if(callback->IsFunction()){
    MakeCallback(ctx->handle_, Persistent<Function>::Cast(callback), 1, args);
    //make sure to remove the callback. Actually don't do that here. 
    //callback.Dispose();
  }else if(callback->IsString()){
    MakeCallback(ctx->handle_, Persistent<String>::Cast(callback), 1, args);
  }else{
    printf("error wtf\n"); 
  }
}

/**
 * void OnSchema(const char* schema)
 * @param schema [description]
 * [OnSchema description]
 */
static void OnSchema(Avro *ctx, const char* schema){
  HandleScope scope;

  assert(ctx->handle_->Get(on_schema)->IsFunction() && "Invalid schema handler");

  Local<Value> args[1] = { String::New(schema) };

  MakeCallback(ctx->handle_, on_schema, 1, args);
}

/**
 * [OnDatum description]
 * @param datum [description]
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
 * [Initialize description]
 * @param target [description]
 */
void Avro::Initialize(Handle<Object> target){
  HandleScope scope;

  Local<FunctionTemplate> a_temp = FunctionTemplate::New(Avro::New);

  a_temp->InstanceTemplate()->SetInternalFieldCount(1);

  NODE_SET_PROTOTYPE_METHOD(a_temp, "decodeFile", Avro::DecodeFile);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "encodeFile", Avro::EncodeFile);

  NODE_SET_PROTOTYPE_METHOD(a_temp, "push", Avro::Push);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "queueSchema", Avro::QueueSchema);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "decodeDatum", Avro::DecodeDatum);
  NODE_SET_PROTOTYPE_METHOD(a_temp, "encodeDatum", Avro::EncodeDatum);


/*
  NODE_SET_PROTOTYPE_METHOD(a_temp, "encodeBytes", Avro::EncodeBytes);
*/
  a_temp->SetClassName(String::NewSymbol("Avro"));
  target->Set(String::NewSymbol("Avro"), a_temp->GetFunction());

  on_schema = NODE_PSYMBOL("onschema");
  on_datum = NODE_PSYMBOL("ondatum");
  on_error = NODE_PSYMBOL("onerror");

}

NODE_MODULE(avro, Avro::Initialize);

}