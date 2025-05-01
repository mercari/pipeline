# Beam SQL Options

Options for beamsql common setting.

## Beam SQL transform module parameters

| parameter              | type    | description                                                                                                                                                                                                                                      |
|------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| plannerName            | Enum    | Specify beam sql planner class name. `org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner` or `org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner`. It can also be set individually in the parameters of the beamsql module. |
| zetaSqlDefaultTimezone | String  | Specify default timezone for zetasql planner. The default is `UTC`                                                                                                                                                                               |
| verifyRowValues        | Boolean | Specify verify row values or not. The default is false                                                                                                                                                                                           |

