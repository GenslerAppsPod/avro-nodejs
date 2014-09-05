#include <node.h>
#include "AvroFileWriter.h"

using namespace v8;
using namespace avro;
using namespace node;


Persistent<Function> AvroFileWriter::constructor;

/**
 *
 * AvroFileWriter is a v8/node wrapper for Avro's DataFileWriter
 * class. simple features are; open, read and close. 
 *
 * @param filename: The name of the file to be decoded.
 */
AvroFileWriter::AvroFileWriter(const char* filename, const char* jsonSchema) {
  std::istringstream is(jsonSchema);
  //create schema

  try{
    compileJsonSchema(is, schema_);
    writer_ = new DataFileWriter<GenericDatum>(filename, schema_);
  }catch(exception &e){
    ThrowException(String::New(e.what()));
  }
  
}

AvroFileWriter::~AvroFileWriter() {

}

void AvroFileWriter::Init(Handle<Object> exports){
  // Prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("AvroFileWriter"));
  tpl->InstanceTemplate()->SetInternalFieldCount(1);
  //Prototype
  NODE_SET_PROTOTYPE_METHOD(tpl, "write", AvroFileWriter::Write);
  NODE_SET_PROTOTYPE_METHOD(tpl, "close", AvroFileWriter::Close);

  constructor = Persistent<Function>::New(tpl->GetFunction());
  exports->Set(String::NewSymbol("AvroFileWriter"), constructor);
 
}

/**
 * constructs a new AvroFileReader 
 *@param filename (an avro data file)
 *
 */
Handle<Value> AvroFileWriter::New(const Arguments& args){
  HandleScope scope;

  if(args.IsConstructCall()){
    String::Utf8Value fileName(args[0]->ToString());
    String::Utf8Value jsonString(args[1]->ToString());
    AvroFileWriter* writer;

    const char* filename = args[0]->IsUndefined() ? "": *(fileName);
    const char* jsonSchema = args[1]->IsUndefined() ? "": *(jsonString);
    writer = new AvroFileWriter(filename,jsonSchema);
    writer->Wrap(args.This());
    return args.This();
  } else {
    const int argc = 1;
    Local<Value> argv[argc] = {args[0]};
    return scope.Close(constructor->NewInstance(argc, argv));
  }
}

/**
 * If there are more data to read in the file. we will grab the next
 * datum. Else we will return nothing.
 *
 * TODO possible throw EOF if we try to read past the end. 
 */
Handle<Value> AvroFileWriter::Write(const Arguments& args){
  HandleScope scope;

  AvroFileWriter *ctx = ObjectWrap::Unwrap<AvroFileWriter>(args.This());
  Local<Value> jsDatum = args[0];

  GenericDatum datum(ctx->schema_);
  datum = DecodeV8(datum, jsDatum);

  ctx->writer_->write(datum); 

  return scope.Close(Undefined());
}
/**
 *
 * Closes the underlining DataFileReader
 *
 */
Handle<Value> AvroFileWriter::Close(const Arguments& args){
  HandleScope scope;

  AvroFileWriter* obj = ObjectWrap::Unwrap<AvroFileWriter>(args.This());

  obj->writer_->flush();
  obj->writer_->close();

  return scope.Close(Undefined());
}
