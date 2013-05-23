#include "avro-node.h"

using namespace v8;
using namespace node;

void InitAll (Handle<Object> target){  
  Init(target);
}

NODE_MODULE(avro, InitAll)