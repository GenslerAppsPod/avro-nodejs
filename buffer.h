#include <stdlib.h>


namespace node {
  

typedef struct BufferBlock_st BufferBlock;

struct BufferBlock_st {
   BufferBlock   *pNextBlock;
   uint8_t *data;
};

class Buffer {
  public:
    Buffer();
    ~Buffer();
    long int appendData(uint8_t *pInput, long int offset, long int dataLength);
    long int readData(
           uint8_t *pOutput,
           long int offset,
           long int arrayLength);
    void freeFirstBlock();
    long int totalLength;
    BufferBlock *pFirstBlock;
    short int startPoint;
    BufferBlock *pLastBlock;
};

}
