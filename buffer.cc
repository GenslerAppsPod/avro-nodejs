#include "buffer.h"



typedef struct BufferBlock_st BufferBlock;

struct BufferBlock_st {
   BufferBlock   *pNextBlock;
   uint8_t *data;
};

class Buffer {
  public:
    /**
     * [newBuffer description]
     * @return [description]
     */
    Buffer() {
      totalLength = 0;
      startPoint = 0;
      pFirstBlock = '\0';
      pLastBlock = pFirstBlock;
    }
    /**
     * [freeBuffer description]
     * @param pBuffer [pointer to the buffer to be freed]
     */
    ~Buffer() {
      /*there is more memory block in the linked list*/
      BufferBlock *block = pFirstBlock;
      while (block) {
        pFirstBlock = block->pNextBlock;
        free(block);
        block = pFirstBlock;
      }
      free(pBuffer);
      //free the Buffer structure;
      return;
    }
    /**
     * [appendData description]
     * @param  pBuffer    [pointer to the abstract buffer]
     * @param  pInput     [pointer to the data source must be null terminated]
     * @param  offset     [offset of the input data]
     * @param  dataLength [number of bytes of the input data]
     * @return            [description]
     */
    long int Buffer::appendData(uint8_t *pInput, long int offset,    
                 long int dataLength) {
      //there is more input data
      BufferBlock *block = (BufferBlock*) malloc(sizeof(BufferBlock));
      
      block->data = pInput;
      if(pLastBlock){
        pLastBlock->pNextBlock = block;
        pLastBlock = block;
      }else{
        pFirstBlock = block;
      }
      totalLength += dataLength;
      
      return totalLength;
    }
    /**
     * [readData description]
     * @param  pBuffer     [pointer to the abstract buffer]
     * @param  pOutput     [pointer to the output byte array]
     * @param  offset      [offset of the output byte array]
     * @param  arrayLength [size of available output byte array]
     * @return             [description]
     */
    long int Buffer::readData(
               uint8_t *pOutput,
               long int offset,
               long int arrayLength
                 ) {
      // while there is something more to read and there is room for output
      BufferBlock *block = pFirstBlock;
      uint8_t byte = '\0';
      while ( byte = block->data[startPoint++] && offset <= arrayLength) {
        totalLength--;
        //read from the first memory block;
        // if the first memory block is empty
        pOutput[offset++] = byte;
        if (!block->data[startPoint]) {
          //delete the first memory block from the linked list and free its memory;
          pFirstBlock = block->pNextBlock;
          free(block);
          block = pFirstBlock;
        }
      }
      return startPoint;
    }

    /**
     * [freeFirstBlock description]
     * @param pBuffer [description]
     */
    void Buffer::freeFirstBlock(){
      BufferBlock *block = pFirstBlock;
      pFirstBlock = block->pNextBlock;
      free(block);
    }
    
    long int totalLength;
    BufferBlock *pFirstBlock;
    short int startPoint;
    BufferBlock *pLastBlock;
};






