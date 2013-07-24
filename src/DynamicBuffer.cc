#include "DynamicBuffer.hh"
#include <cstring>
namespace avronode {

  /**
   * [newBuffer description]
   * @return [description]
   */
  DynamicBuffer::DynamicBuffer() {
    totalLength = 0;
    startPoint = 0;
    numBlocks = 0;
    pFirstBlock = (BufferBlock*) malloc(sizeof(BufferBlock));
    pFirstBlock->data = (uint8_t*)malloc(sizeof(uint8_t)*1);
    pLastBlock = pFirstBlock;
  }
  /**
   * [freeBuffer description]
   * @param pBuffer [pointer to the buffer to be freed]
   */
  DynamicBuffer::~DynamicBuffer() {
    /*there is more memory block in the linked list*/
    BufferBlock *block = pFirstBlock;
    while (block) {
      pFirstBlock = block->pNextBlock;
      //free(block);
      //free(block->data);
      block = pFirstBlock;
    }
    //free(pBuffer);
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
  long int DynamicBuffer::appendData(uint8_t *pInput, long int offset,    
               long int dataLength) {
    //there is more input data
    BufferBlock *block = (BufferBlock*) malloc(sizeof(BufferBlock));
    block->data = (uint8_t*)malloc(sizeof(uint8_t)*dataLength); 
    memcpy(block->data, pInput, dataLength);
    block->size = dataLength;
    block->pNextBlock = NULL;
    numBlocks++;

    if(pFirstBlock == NULL){
      pFirstBlock = block;
      pLastBlock = block;
    }else if(pFirstBlock != NULL){
      pLastBlock->pNextBlock = block;
      pLastBlock = block;
    }
    totalLength += dataLength;

    
    return totalLength;
  }
  /**
   * [readData description]
   * @param  pOutput     [pointer to the output byte array]
   * @param  offset      [offset of the output byte array]
   * @param  arrayLength [size of available output byte array]
   * @return             [description]
   */
  long int DynamicBuffer::readData(uint8_t **pOutput,
             long int offset,
             long int arrayLength) {
    // while there is something more to read and there is room for output
    BufferBlock *block = pFirstBlock;
    
    uint8_t *byte = '\0';
   
    if(block) {

      byte = &(block->data[startPoint++]);
      totalLength--;

      //read from the first memory block;
      // if the first memory block is empty
      (pOutput[offset++]) = byte;
      if (block->size == startPoint) {
        printf("Block num: %d\n", numBlocks);
        //delete the first memory data[startPoint]block from the linked list and free its memory;
        pFirstBlock = block->pNextBlock;
        realloc(block->data, block->size * sizeof(uint8_t));
        free(block);
        startPoint = 0;
        numBlocks--;
        return startPoint;
      }
    }

    return startPoint;
  }

  /**
   * [DynamicBuffer::readBlock description]
   * @param  pOutput [description]
   * @return         [description]
   */
  long int DynamicBuffer::readBlock(uint8_t **pOutput){
    BufferBlock *block = pFirstBlock->pNextBlock;

    free(pFirstBlock->data);
    free(pFirstBlock);
    int blockSize = 0;

    if(block){
      blockSize = block->size;
      //printf("Block num: %d\n", numBlocks);
      *pOutput = block->data;
      totalLength -= blockSize;

      pFirstBlock = block;
      //free(block->data);
      numBlocks--;
    }
    return blockSize;
  }

  /**
   * [freeFirstBlock description]
   * @param pBuffer [description]
   */
  void DynamicBuffer::freeFirstBlock(){
    BufferBlock *block = pFirstBlock;
    pFirstBlock = block->pNextBlock;
    //free(block);
  }

}






