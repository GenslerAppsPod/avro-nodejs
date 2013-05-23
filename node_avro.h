#include <node.h>
#include <node_buffer.h>
#include <avro/ValidSchema.hh>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <avro/Compiler.hh>

using namespace node;

typedef std::vector<char> buffer_type;


class Avro : public ObjectWrap
{
public:


private: 
  Avro() : ObjectWrap(){};
  ~Avro(){};
  static Handle<Value> New(const Arguments& args);
  static Handle<Value> DecodeFile(const Arguments &args);
  static Handle<Value> DecodeBytes(const Arguments &args);  
  static Handle<Value> EncodeFile(const Arguments &args);
  static Handle<Value> EncodeBytes(const Arguments &args);
  ValidSchema schema_;
  buffer_type buffer_;
  bool write_in_progress_;


}