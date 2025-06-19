

```bash
protoc src/main/resources/schema/protobuf/auxia/auxia.proto \
  --descriptor_set_out=src/main/resources/schema/protobuf/auxia/auxia.desc \
  --include_imports \
  --include_source_info
```