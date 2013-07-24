#include <v8.h>
#include <node.h>
#include <node_buffer.h>
#include <avro/ValidSchema.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>

void unionBranch(avro::GenericDatum *datum, const char *type);

avro::GenericDatum DecodeV8(avro::GenericDatum datum, v8::Local<v8::Value> object);

v8::Handle<v8::Value> DecodeAvro(const avro::GenericDatum& datum);