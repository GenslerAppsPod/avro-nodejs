#include <stdlib.h>
#include <v8.h>
#include <pthread.h>

namespace avronode {

typedef struct BufferBlock_st BufferBlock;

struct BufferBlock_st {
   BufferBlock   *pNextBlock;
   uint8_t *data;
   int long size;
};

class DynamicBuffer {
  public:
    DynamicBuffer();
    ~DynamicBuffer();
    long int appendData(uint8_t *pInput, long int offset, long int dataLength);
    long int readData(
           uint8_t **pOutput,
           long int offset,
           long int arrayLength);
    long int readBlock(uint8_t **pOutput);
    void freeFirstBlock();
    long int totalLength;
    BufferBlock *pFirstBlock;
    short int startPoint;
    BufferBlock *pLastBlock;
    int numBlocks;
};

}
