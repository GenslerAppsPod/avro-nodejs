{
  "targets": [
    {
      "target_name": "avro",
      "type": "loadable_module",
      "product_extension": "node",
      'include_dirs': ['/home/19567/Workspace/avro-nodejs/avrocpp/include', "./"],
      'cflags_cc!': ['-fno-rtti', '-fno-exceptions'],
      'cflags_cc+': ['-frtti', '-fexceptions'],
      'link_settings': {
        'ldflags': ['-L/home/19567/Workspace/avro-nodejs/avrocpp/lib'],
        'libraries': ['/home/19567/Workspace/avro-nodejs/avrocpp/lib/libavrocpp.so']
      },
      "sources": [ "./src/node_avro.cc","./src/DynamicBuffer.cc", "./src/BufferedInputStream.cc", "./src/translate.cc"]
    }
  ]
}
