{
  "targets": [
    {
      "target_name": "avro",
      "type": "loadable_module",
      "product_extension": "node",
      'include_dirs': ['/usr/local/include'],
      'cflags_cc!': ['-fno-rtti', '-fno-exceptions'],
      'cflags_cc+': ['-frtti', '-fexceptions'],
      'link_settings': {
        'ldflags': ['-L/usr/local/lib'],
        'libraries': ['/usr/local/lib/libavrocpp.so']
      },
      "sources": [ "node_avro.cc", "BufferedInputStream.cc", "translate.cc"]
    }
  ]
}
