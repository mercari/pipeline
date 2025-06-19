# Define system config

`system` defines pipeline system configurations.
The following items can be defined as system.

| parameter | type                          | description                                                                                      |
|-----------|-------------------------------|--------------------------------------------------------------------------------------------------|
| args      | Map<String,String\>           | Default values of variables at pipeline startup, which can also be rewritten by Template Engine  |
| context   | String                        | Specify pipeline execution path.                                                                 |
| imports   | Array<Import\>                | Configuration to import external config files.                                                   |
| failure   | Failure                       | Configure handling of failed processes.                                                          |


```yaml
system:
  args:
    arg1: ""
    arg2: ""
  context: ""
  imports:
    - base: ""
      files:
        - ""
  failure:
    failFast: true
```