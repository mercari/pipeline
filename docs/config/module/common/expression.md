# Expression

This function evaluates data by assigning them to an expression given by a formula.
In a formula, you can specify field names for the data, and the values will be assigned at runtime.
All field values are converted to `double` type according to the schema of the original data.
The following built-in operators and functions are available for use in formulas.

## Build-in operators

### numeric operators

| function | description    |
|----------|----------------|
| `+`      | Addition       |
| `-`      | Subtraction    |
| `*`      | Multiplication |
| `/`      | Division       |
| `%`      | Modulo         |
| `^`      | Exponentiation |

### Comparison and logical operators

| function | description            |
|----------|------------------------|
| `=`      | Equals                 |
| `!=`     | Not equals             |
| `>`      | Greater than           |
| `>=`     | Greater than or equals |
| `<`      | Less than              |
| `<=`     | Less than or equals    |
| `!`      | NOT                    |
| `&`      | AND                    |
| `&#124;` | OR                     |



## Build-in functions

### single argument functions

| function         | description                              |
|------------------|------------------------------------------|
| abs(`value`)     | absolute value                           |
| acos(`radian`)   | arc cosine                               |
| asin(`radian`)   | arc sine                                 |
| atan(`radian`)   | arc tangent                              |
| cbrt(`value`)    | cubic root                               |
| ceil(`value`)    | nearest upper integer                    |
| cos(`radian`)    | cosine                                   |
| cosh(`radian`)   | hyperbolic cosine                        |
| exp(`value`)     | euler's number raised to the power (e^x) |
| floor(`value`)   | nearest lower integer                    |
| log(`value`)     | logarithmus naturalis (base e)           |
| log10(`value`)   | logarithm (base 10)                      |
| log2(`value`)    | logarithm (base 2)                       |
| sin(`radian`)    | sine                                     |
| sinh(`radian`)   | hyperbolic sine                          |
| sqrt(`value`)    | square root                              |
| tan(`radian`)    | tangent                                  |
| tanh(`radian`)   | hyperbolic tangent                       |
| signum(`value`)  | signum function                          |

### multi arguments functions

| function                                                      | description                                                                                          |
|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| if(`expr`, `true_result`, `else_result`)                      | If `expr` evaluates to `true`, returns `true_result`, else returns the evaluation for `else_result`. |
| max(`value1`, `value2`)                                       | Returns the greater of two arguments values.                                                         |
| min(`value1`, `value2`)                                       | Returns the smaller of two arguments values.                                                         |
| min(`value1`, `value2`)                                       | Returns the smaller of two arguments values.                                                         |
| timestamp_diff_day(`timestamp_value2`, `timestamp_value2`)    | Returns the difference between two timestamps in days                                                |
| timestamp_diff_hour(`timestamp_value2`, `timestamp_value2`)   | Returns the difference between two timestamps in hours                                               |
| timestamp_diff_minute(`timestamp_value2`, `timestamp_value2`) | Returns the difference between two timestamps in minutes                                             |
| timestamp_to_date(`timestamp_value`, `offset_hour`)           | Converts a timestamp value to a date with the specified offset hour                                  |


## Data type mapping

With expression, data fields types are converted to `double` types with the following mapping.


| original data type  | description                                                                        |
|---------------------|------------------------------------------------------------------------------------|
| int16, int32, int64 | integer value as double value                                                      |
| float32, float64    | double value itself                                                                |
| numeric             | bigdecimal value as double value                                                   |
| boolean             | `1.0` if the value is true, `0.0` if false                                         |
| string              | Parse string value as double type                                                  |
| date                | double value of unix epoch days (days from `1970-01-01`)                           |
| timestamp           | double value of unix epoch microseconds (microseconds from `1970-01-01T00:00:00Z`) |

## Numerical constants

The following constants can be used in expressions

* `pi`, `π`: the value of π as defined in Math.PI
* `e`: the value of Euler's number e
